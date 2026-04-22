"""
NetX360 Performance Scraper v3 — 2026-04-14

Cambios vs v2:
  - Lista de cuentas dinámica desde allbalances-ibdoffip (fallback: Excel en Downloads)
  - Cierra pestañas residuales mainaccount.com antes de expect_page()
  - Escape antes/después del mat-select rgl para limpiar overlays Angular
  - SB_KEY desde variable de entorno (fallback al valor hardcodeado)
  - SSL verificación habilitada en llamadas a Supabase
  - Año de ytd dinámico (no hardcodeado a 2026)
  - parse_pct: regex ≤2 decimales — ignora dígitos de footnote appended
"""

import asyncio
import json
import math
import os
import re
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from playwright.async_api import async_playwright


# ── Config ────────────────────────────────────────────────────────────────────
OUTPUT_FILE  = os.path.expanduser(f"~/Downloads/ofir_performance_v3_{date.today().isoformat()}.json")
TODAY_ISO    = date.today().isoformat()
ACCT_TIMEOUT = 180
SAVE_EVERY   = 5

SB_URL = 'https://jpegcujzyzqfjvlumzpm.supabase.co'
# service_role key — bypasses RLS, permite escribir nombres reales
SB_KEY = os.environ.get(
    'SB_KEY',
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImpwZWdjdWp6eXpxZmp2bHVtenBtIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NDYzODM3MSwiZXhwIjoyMDkwMjE0MzcxfQ.BfIijxmwi_8JZoXm6voQADVkxn18IFwzTVkuZZiV_J0',
)

ALLBALANCES_URL = 'https://www2.netx360.com/plus/my-practice/allaccounts/allbalances-ibdoffip'


# ── Dynamic account list ──────────────────────────────────────────────────────

async def get_accounts_from_page(page):
    """
    Scrape (Short Name, Account #, Account Value) from allbalances-ibdoffip.
    Handles ag-grid (NetX360's usual renderer) and plain <table> fallback.
    Returns list of (name, account_id, value) sorted by value desc.
    """
    print("→ Cargando lista de cuentas desde allbalances...")
    await page.goto(ALLBALANCES_URL, wait_until='domcontentloaded', timeout=30000)
    await page.wait_for_timeout(5000)

    accounts = await page.evaluate("""
    () => {
        const JXD = /^JXD\\d{6}$/;

        function parseVal(s) {
            if (!s) return 0;
            const n = parseFloat(String(s).replace(/[$,]/g, ''));
            return isNaN(n) ? 0 : n;
        }

        // ── ag-grid ───────────────────────────────────────────────────────
        const agRows = document.querySelectorAll('.ag-center-cols-container .ag-row');
        if (agRows.length > 0) {
            const results = [];
            agRows.forEach(row => {
                const cells = {};
                row.querySelectorAll('.ag-cell[col-id]').forEach(c => {
                    cells[c.getAttribute('col-id')] = c.innerText.trim();
                });
                // Try to find account# cell (any col whose value matches JXD pattern)
                let acctId = null, name = null, val = 0;
                for (const [k, v] of Object.entries(cells)) {
                    if (JXD.test(v)) { acctId = v; }
                }
                if (!acctId) return;
                // Short Name is often the first text col; Account Value is numeric
                for (const [k, v] of Object.entries(cells)) {
                    if (k.toLowerCase().includes('short') || k.toLowerCase().includes('name')) name = v;
                    if (k.toLowerCase().includes('value') || k.toLowerCase().includes('market')) {
                        const n = parseVal(v);
                        if (n !== 0) val = n;
                    }
                }
                // Fallback: first non-JXD, non-numeric cell → name
                if (!name) {
                    const vals = Object.values(cells);
                    name = vals.find(v => v && !JXD.test(v) && isNaN(parseVal(v))) || acctId;
                }
                results.push([name || acctId, acctId, val]);
            });
            if (results.length > 0) return {source: 'ag-grid', rows: results};
        }

        // ── plain <table> ─────────────────────────────────────────────────
        for (const table of document.querySelectorAll('table')) {
            const headers = Array.from(table.querySelectorAll('th')).map(th => th.innerText.trim().toLowerCase());
            const nameIdx  = headers.findIndex(h => h.includes('short') || h.includes('name'));
            const acctIdx  = headers.findIndex(h => h.includes('account') && (h.includes('#') || h.includes('num')));
            const valIdx   = headers.findIndex(h => h.includes('value') || h.includes('market'));
            if (acctIdx < 0) continue;
            const results = [];
            table.querySelectorAll('tr').forEach(tr => {
                const tds = Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim());
                if (!tds[acctIdx] || !JXD.test(tds[acctIdx])) return;
                results.push([
                    nameIdx >= 0 ? tds[nameIdx] : tds[acctIdx],
                    tds[acctIdx],
                    valIdx >= 0 ? parseVal(tds[valIdx]) : 0,
                ]);
            });
            if (results.length > 0) return {source: 'table', rows: results};
        }

        // ── last resort: scan all text for JXD IDs ────────────────────────
        const allText = document.body.innerText;
        const ids = [...new Set(allText.match(/JXD\\d{6}/g) || [])];
        return {source: 'text-scan', rows: ids.map(id => [id, id, 0])};
    }
    """)

    rows = accounts.get('rows', [])
    print(f"  Fuente: {accounts.get('source')} — {len(rows)} cuentas encontradas")

    if not rows:
        raise RuntimeError("No se pudo extraer la lista de cuentas desde allbalances.")

    # Sort by value desc, same as ACCOUNTS_RAW
    rows.sort(key=lambda x: x[2], reverse=True)
    return [(name, acct_id, val) for name, acct_id, val in rows]


def get_accounts_from_excel():
    """
    Fallback: read from most recent Account+List Excel in ~/Downloads.
    Columns: Short Name (A), Account # (B), Account Value (C), header at row 17 (index 16).
    """
    import glob, openpyxl
    pattern = os.path.expanduser('~/Downloads/Account+List_JXD_ALL_VJ9*.xlsx')
    files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"No se encontró archivo Excel en ~/Downloads/ con patrón Account+List_JXD_ALL_VJ9*.xlsx")
    path = files[0]
    print(f"  Leyendo cuentas desde Excel: {Path(path).name}")
    wb = openpyxl.load_workbook(path)
    ws = wb.active
    rows = []
    for row in ws.iter_rows(min_row=18, values_only=True):  # data starts at row 18
        name, acct_id, val = row[0], row[1], row[2]
        if not acct_id or not str(acct_id).startswith('JXD'):
            continue
        rows.append((str(name or acct_id), str(acct_id), float(val or 0)))
    rows.sort(key=lambda x: x[2], reverse=True)
    print(f"  {len(rows)} cuentas leídas del Excel")
    return rows


# ── Helpers ───────────────────────────────────────────────────────────────────

def compound(pct_list):
    vals = [p for p in pct_list if p is not None]
    if not vals:
        return None
    r = 1.0
    for p in vals:
        r *= 1 + p / 100.0
    return round((r - 1) * 100, 4)


def parse_number(text):
    if not text:
        return None
    t = str(text).strip().lstrip('$').replace(',', '').replace('%', '').strip()
    if t in ('--', '-', '', 'N/A', 'n/a'):
        return None
    try:
        return float(t)
    except ValueError:
        return None


def parse_pct(text):
    """Parse % value; matches at most 2 decimal places so appended footnote digits are ignored.
    e.g. '0.9018' (footnote '18') → 0.90,  '3.25%' → 3.25,  '-1.5' → -1.5
    """
    if not text:
        return None
    t = str(text).strip().replace('%', '').strip()
    if t in ('--', '-', '', 'N/A', 'n/a'):
        return None
    m = re.match(r'^(-?\d+(?:\.\d{1,2})?)', t)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


DATE_RE        = re.compile(r'(\d{1,2}/\d{1,2}/\d{2,4})\s*[-–]\s*(\d{1,2}/\d{1,2}/\d{2,4})')
SINGLE_DATE_RE = re.compile(r'^(\d{1,2}/\d{1,2}/\d{2,4})\*?$')

def _to_date(s):
    p = s.strip().split('/')
    mo, dy, yr = int(p[0]), int(p[1]), int(p[2])
    if yr < 100: yr += 2000
    return date(yr, mo, dy)

def parse_date_range(text):
    t = str(text)
    m = DATE_RE.search(t)
    if m:
        try:
            return _to_date(m.group(1)), _to_date(m.group(2))
        except Exception:
            return None
    m2 = SINGLE_DATE_RE.match(t.strip())
    if m2:
        try:
            d = _to_date(m2.group(1))
            return d, d
        except Exception:
            return None
    return None


# ── Table parser ──────────────────────────────────────────────────────────────

def parse_table(rows):
    portfolio_col = ending_col = net_col = change_col = None
    for row in rows:
        joined = ' '.join(row).lower()
        if 'portfolio' in joined or 'ending value' in joined:
            for ci, h in enumerate(row):
                hl = h.lower().strip()
                if 'portfolio' in hl and '(' in hl and portfolio_col is None:
                    portfolio_col = ci
                if 'ending value' in hl and ending_col is None:
                    ending_col = ci
                if 'net contribution' in hl and net_col is None:
                    net_col = ci
                if 'change in value' in hl and change_col is None:
                    change_col = ci
            if portfolio_col is not None:
                break

    if ending_col    is None: ending_col    = 1
    if net_col       is None: net_col       = 2
    if change_col    is None: change_col    = 3
    if portfolio_col is None: portfolio_col = 4

    print(f"      Cols: ending={ending_col}, net={net_col}, change={change_col}, portfolio={portfolio_col}")

    def get(row, col):
        return row[col] if col < len(row) else None

    all_date_rows = []
    for row in rows:
        if not row or not row[0].strip():
            continue
        col0 = row[0].strip()
        dr = parse_date_range(col0)
        if dr is None:
            continue
        start_d, end_d = dr
        span = (end_d - start_d).days
        is_partial = '*' in col0 or start_d == end_d
        pct = parse_pct(get(row, portfolio_col))
        all_date_rows.append(dict(start=start_d, end=end_d, pct=pct,
                                  partial=is_partial, span=span, row=row, raw=col0))

    total_row = None
    if all_date_rows:
        total_row = max(all_date_rows, key=lambda x: x['span'])

    date_rows = [d for d in all_date_rows if d is not total_row]

    print(f"      Total row: {total_row['raw'] if total_row else None}")
    print(f"      Period rows: {len(date_rows)}")
    for dr in sorted(date_rows, key=lambda x: x['end']):
        print(f"        {dr['start']} → {dr['end']}  pct={dr['pct']}  partial={dr['partial']}")

    ending_value = net_contribution = change_in_value = None
    if total_row:
        r = total_row['row']
        ending_value     = parse_number(get(r, ending_col))
        net_contribution = parse_number(get(r, net_col))
        change_in_value  = parse_number(get(r, change_col))

    since_start = total_row['pct'] if total_row else None

    full_rows    = sorted([d for d in date_rows if not d['partial']], key=lambda x: x['start'])
    partial_rows = sorted([d for d in date_rows if d['partial']],     key=lambda x: x['end'])

    all_period_sorted = sorted(date_rows, key=lambda x: x['end'])
    m1 = all_period_sorted[-1]['pct'] if all_period_sorted else None

    # ytd: all period rows in the current calendar year
    cur_year = date.today().year
    ytd_rows = [d for d in date_rows if d['start'].year == cur_year or d['end'].year == cur_year]
    ytd = compound([d['pct'] for d in ytd_rows])

    # y2025 / y2024: rows entirely within those years
    y2025_rows = [d for d in date_rows if d['start'].year == 2025 and d['end'].year <= 2025]
    y2025 = compound([d['pct'] for d in y2025_rows])

    y2024_rows = [d for d in date_rows if d['start'].year == 2024 and d['end'].year <= 2024]
    y2024 = compound([d['pct'] for d in y2024_rows])

    y3 = None
    if y2024_rows and y2025_rows:
        y3 = compound([d['pct'] for d in y2024_rows + y2025_rows + ytd_rows])

    def quarter_of(d):
        return (d.year, (d.month - 1) // 3 + 1)

    qmap = defaultdict(list)
    for d in full_rows:
        qmap[quarter_of(d['end'])].append(d)

    all_quarters = []
    for qk in sorted(qmap.keys()):
        qret = compound([d['pct'] for d in qmap[qk]])
        all_quarters.append((qk, qret))

    current_q = quarter_of(date.today())
    complete_q = [(qk, qr) for qk, qr in all_quarters if qk < current_q]

    m3 = complete_q[-1][1]  if complete_q            else None
    m6 = compound([qr for _, qr in complete_q[-2:]]) if len(complete_q) >= 2 else None

    # vol: annualized from quarterly returns (sqrt(4) = 2)
    vol = None
    q_rets = [qr for _, qr in complete_q if qr is not None]
    if len(q_rets) >= 2:
        mean = sum(q_rets) / len(q_rets)
        var  = sum((r - mean) ** 2 for r in q_rets) / (len(q_rets) - 1)
        vol  = round(math.sqrt(var) * 2, 4)

    start_date = total_row['start'].isoformat() if total_row else None

    history = []
    for d in sorted(all_date_rows, key=lambda x: x['end']):
        if d is total_row:
            continue
        # Descartar registros con fechas invertidas (bug de NetX360 en cuentas nuevas)
        if d['end'] < d['start']:
            print(f"      ⚠️  Ignorando período con fechas invertidas: {d['start']} → {d['end']}")
            continue
        r_row = d['row']
        history.append({
            'period_start':    d['start'].isoformat(),
            'period_end':      d['end'].isoformat(),
            'ending_value':    parse_number(get(r_row, ending_col)),
            'net_contribution': parse_number(get(r_row, net_col)),
            'pct':             d['pct'],
            'partial':         d['partial'],
        })

    return dict(ending_value=ending_value, net_contribution=net_contribution,
                change_in_value=change_in_value, ytd=ytd, y2025=y2025, y2024=y2024,
                y3=y3, m1=m1, m3=m3, m6=m6, since_start=since_start, vol=vol,
                start_date=start_date, history=history)


# ── Per-account scraper ───────────────────────────────────────────────────────

def empty_result(account_id, name, value):
    return dict(account=account_id, name=name, value=value,
                ending_value=None, net_contribution=None, change_in_value=None,
                ytd=None, y2025=None, y2024=None, y3=None,
                m1=None, m3=None, m6=None, since_start=None, vol=None,
                start_date=None, as_of=TODAY_ISO,
                positions_text=None, unrealized_text=None, realized_text=None, history=[])


async def scrape_account(context, search_page, account_id, name, value):
    print(f"\n{'='*60}")
    print(f"  {account_id} | {name} | ${value:,.2f}")
    print(f"{'='*60}")

    new_tab = None
    result  = empty_result(account_id, name, value)

    try:
        # ── PASO 1: buscar cuenta via global search ───────────────────────
        await search_page.goto(
            "https://www2.netx360.com/plus/my-practice/details/albridge-performance-account",
            wait_until='domcontentloaded', timeout=20000
        )
        await search_page.wait_for_timeout(1500)
        await search_page.evaluate(
            "document.querySelectorAll('.cdk-overlay-backdrop, .cdk-overlay-container')"
            ".forEach(el => { try { el.click(); } catch(e) {} })"
        )
        await search_page.wait_for_timeout(300)
        await search_page.keyboard.press('Escape')
        await search_page.wait_for_timeout(300)

        print(f"  → Opening search modal...")
        await search_page.click("input[placeholder='Search']", timeout=10000, force=True)
        await search_page.wait_for_timeout(800)

        print(f"  → Typing {account_id} in #global-search...")
        search_input = search_page.locator('#global-search')
        await search_input.wait_for(timeout=8000)
        await search_input.click(force=True)
        await search_page.wait_for_timeout(300)
        await search_input.press_sequentially(account_id, delay=80)
        await search_page.wait_for_timeout(2500)

        print(f"  → Clicking search result {account_id}...")
        await search_page.locator(f'strong:text("{account_id}")').first.click(timeout=15000)
        await search_page.wait_for_timeout(1500)

        title_after = await search_page.title()
        print(f"  → Page after search click: {title_after!r}")

        # ── PASO 2: cerrar tabs residuales de mainaccount.com ─────────────
        # Si quedan pestañas de mainaccount.com de la cuenta anterior,
        # context.expect_page() las capturaría a ellas en vez de la nueva.
        residual = [p for p in context.pages if 'mainaccount.com' in p.url]
        if residual:
            print(f"  → Cerrando {len(residual)} pestaña(s) residual(es) de mainaccount.com...")
            for p in residual:
                try:
                    await p.close()
                except Exception:
                    pass
            await asyncio.sleep(0.5)

        # ── PASO 3: click Performance → nueva pestaña ─────────────────────
        print(f"  → Clicking Performance (expecting new tab)...")
        async with context.expect_page(timeout=40000) as new_page_info:
            perf_links = search_page.locator("a:has-text('Performance')")
            count = await perf_links.count()
            print(f"  → Found {count} Performance link(s)")
            await perf_links.last.click()

        new_tab = await new_page_info.value

        print(f"  → Waiting for mainaccount.com...")
        for _ in range(40):
            await asyncio.sleep(1)
            if 'mainaccount.com' in new_tab.url:
                break
        print(f"  → Tab URL: {new_tab.url}")

        if 'mainaccount.com' not in new_tab.url:
            raise Exception(f"Performance tab never reached mainaccount.com — got: {new_tab.url}")

        await new_tab.wait_for_load_state('networkidle', timeout=30000)
        await new_tab.wait_for_selector('#reportType', timeout=20000)
        print(f"  ✓ Report page loaded")

        # ── PASO 4: setear dropdowns y Preview ───────────────────────────
        await new_tab.select_option('select#reportType', value='63')
        await new_tab.wait_for_timeout(500)
        await new_tab.select_option('select#reportTimePeriod', value='21')
        await new_tab.wait_for_timeout(500)

        vals = await new_tab.evaluate(
            "() => ({ rt: document.querySelector('#reportType').value,"
            "         rtp: document.querySelector('#reportTimePeriod').value })"
        )
        print(f"  → Dropdowns set: {vals}")

        print(f"  → Clicking Preview...")
        await new_tab.click("input[value='Preview']")

        try:
            await new_tab.wait_for_load_state('networkidle', timeout=15000)
        except Exception:
            print(f"  ⚠ networkidle timeout")

        print(f"  → Waiting for table.r_table...")
        await new_tab.wait_for_selector('table.r_table', timeout=15000)
        print(f"  ✓ Table found")

        # ── PASO 5: extraer filas (text nodes only, sin superíndices) ─────
        rows = await new_tab.evaluate("""
            () => {
                const tables = Array.from(document.querySelectorAll('table.r_table'));
                let target = null;
                for (const t of tables) {
                    const txt = t.innerText || '';
                    if (txt.includes('PORTFOLIO') || txt.includes('PERIOD')) {
                        target = t; break;
                    }
                }
                if (!target) target = tables[0];
                if (!target) return [];
                return Array.from(target.querySelectorAll('tr')).map(tr =>
                    Array.from(tr.querySelectorAll('td,th')).map(td => {
                        let txt = '';
                        td.childNodes.forEach(n => {
                            if (n.nodeType === 3) txt += n.textContent;
                        });
                        return (txt.trim() || td.innerText.trim());
                    })
                );
            }
        """)

        print(f"  → {len(rows)} rows extracted")
        for i, row in enumerate(rows):
            print(f"    [{i:02d}] {row}")

        # ── PASO 6: parsear ───────────────────────────────────────────────
        metrics = parse_table(rows)
        result  = {**empty_result(account_id, name, value), **metrics}

        print(f"\n  ✓ RESULT:")
        for k in ['ending_value', 'net_contribution', 'change_in_value',
                  'since_start', 'ytd', 'y2025', 'y2024', 'y3',
                  'm1', 'm3', 'm6', 'vol']:
            print(f"    {k:20} = {result[k]}")

    except Exception as e:
        print(f"  ✗ ERROR: {e}")
        import traceback; traceback.print_exc()

    finally:
        if new_tab:
            try:
                await new_tab.close()
                print(f"  → Tab closed")
            except Exception:
                pass

        os.makedirs(os.path.expanduser("~/Downloads/netx360_debug"), exist_ok=True)

        # ── Positions ─────────────────────────────────────────────────────
        print(f"  → Positions...")
        try:
            # Viewport alto para que AG Grid renderice todas las filas de una vez
            # (el chart encima del grid consume ~500px; con 2000px sobra para ~35 filas)
            await search_page.set_viewport_size({"width": 1280, "height": 2000})
            await search_page.goto(
                "https://www2.netx360.com/plus/my-practice/details/positions-account",
                wait_until='domcontentloaded', timeout=30000
            )
            try:
                await search_page.wait_for_selector(
                    '.ag-center-cols-container .ag-row', timeout=12000
                )
                await search_page.wait_for_timeout(800)
            except Exception:
                await search_page.wait_for_timeout(6000)
            with open(os.path.expanduser(f"~/Downloads/netx360_debug/{account_id}_positions.html"), "w") as f:
                f.write(await search_page.content())
            result['positions_text'] = (await search_page.inner_text("body"))[:15000]
            print(f"    ✓ positions: {len(result['positions_text'])} chars")

            # Total Account Value from summary banner — fuente más confiable que Albridge
            # (incluye cash/new money que Albridge puede tardar en reflejar)
            try:
                tav = await search_page.evaluate("""
                    () => {
                        const items = document.querySelectorAll('.acs-SummaryCard_Item');
                        for (const item of items) {
                            const label = item.querySelector('.label');
                            const value = item.querySelector('.value');
                            if (label && value && label.textContent.includes('Total Account Value')) {
                                const v = parseFloat(value.textContent.replace(/[,$]/g, ''));
                                return isNaN(v) ? null : v;
                            }
                        }
                        return null;
                    }
                """)
                if tav:
                    albridge_ev = result.get('ending_value')
                    if albridge_ev and abs(tav - albridge_ev) / albridge_ev > 0.01:
                        print(f"    ⚠ TAV {tav:,.2f} difiere de Albridge {albridge_ev:,.2f} — usando TAV")
                    else:
                        print(f"    ✓ Total Account Value: {tav:,.2f}")
                    result['ending_value'] = tav
            except Exception as e:
                print(f"    ✗ TAV extraction error: {e}")

        except Exception as e:
            print(f"    ✗ positions error: {e}")
        finally:
            await search_page.set_viewport_size({"width": 1280, "height": 1080})

        # ── Unrealized G/L ────────────────────────────────────────────────
        print(f"  → Unrealized G/L...")
        try:
            await search_page.goto(
                "https://www2.netx360.com/plus/my-practice/details/ugl-account",
                wait_until='domcontentloaded', timeout=30000
            )
            # Esperar a que el AG Grid cargue filas reales (no solo los headers)
            try:
                await search_page.wait_for_selector(
                    '.ag-center-cols-container .ag-row', timeout=12000
                )
                await search_page.wait_for_timeout(800)
            except Exception:
                await search_page.wait_for_timeout(6000)
            with open(os.path.expanduser(f"~/Downloads/netx360_debug/{account_id}_unrealized.html"), "w") as f:
                f.write(await search_page.content())
            result['unrealized_text'] = (await search_page.inner_text("body"))[:12000]
            print(f"    ✓ unrealized: {len(result['unrealized_text'])} chars")
        except Exception as e:
            print(f"    ✗ unrealized error: {e}")

        # ── Realized G/L Prior Year ───────────────────────────────────────
        print(f"  → Realized G/L Prior Year...")
        try:
            await search_page.goto(
                "https://www2.netx360.com/plus/my-practice/details/rgl-account",
                wait_until='domcontentloaded', timeout=30000
            )
            await search_page.wait_for_timeout(3000)

            # Limpiar cualquier overlay Angular antes de intentar el dropdown
            await search_page.keyboard.press('Escape')
            await search_page.wait_for_timeout(400)

            try:
                mat_sel = search_page.locator('mat-select').first
                await mat_sel.click()
                await search_page.wait_for_timeout(1000)
                opt = search_page.locator('mat-option').filter(has_text='Prior Year')
                await opt.first.click()
                await search_page.wait_for_timeout(2000)
            except Exception as e2:
                print(f"    ⚠ dropdown: {e2}")
            finally:
                # Siempre cerrar el overlay, haya fallado o no
                await search_page.keyboard.press('Escape')
                await search_page.wait_for_timeout(300)

            with open(os.path.expanduser(f"~/Downloads/netx360_debug/{account_id}_realized.html"), "w") as f:
                f.write(await search_page.content())
            result['realized_text'] = (await search_page.inner_text("body"))[:8000]
            print(f"    ✓ realized: {len(result['realized_text'])} chars")
        except Exception as e:
            print(f"    ✗ realized error: {e}")

        # Limpiar estado para la próxima cuenta
        try:
            await search_page.goto(
                "https://www2.netx360.com/plus/my-practice/details/albridge-performance-account",
                wait_until='domcontentloaded', timeout=20000
            )
            await search_page.wait_for_timeout(1000)
        except Exception:
            pass

        return result


# ── Positions / Unrealized GL parsers ────────────────────────────────────────

_ISIN_RE     = re.compile(r'^[A-Z]{2}[A-Z0-9]{10}$')
_MATURITY_RE = re.compile(r'^[A-Z][a-z]{2}\s+\d{1,2},\s+\d{4}$')
_MONEY_RE    = re.compile(r'^\d{1,3}(?:,\d{3})*\.\d{2}$')
_SIGNED_RE   = re.compile(r'^-?\d{1,3}(?:,\d{3})*\.\d{2}$')
_DEC4_RE     = re.compile(r'^\d{1,3}(?:,\d{3})*\.\d{4}$')

_CATEGORIES = [
    ('Liquidez',       ['MONEY MARKET', 'BNY MELLON U.S. TREASURY', 'DUTG', 'U.S.DOLLARS CURRENCY']),
    ('Alternativos',   ['GOLD', 'GLD', 'BITCOIN', 'IBIT', 'ETHEREUM', 'ETHA', 'GRAYSCALE',
                        'CAT BOND', 'ATLAS TITAN', 'PM ALPHA DAC', 'BARINGS BPCC ETN',
                        'NEUBERGER BERMAN GLOBAL PRIVATE EQUITY',
                        'LKD TO', 'STRUCTURED',
                        'JANUS HENDERSON ABSOLUTE', 'JANUS HENDERSON BALANCED',
                        'JUPITER MERIAN',
                        'PIMCO BALANCED INCOME AND GROWTH',
                        'SCHRODER CAPITAL',
                        'COPPER MINERS']),
    ('Renta Variable', ['EQUITY', 'EQUITIES', 'SMALLER COMPANIES', 'PREMIUM EQUITIES',
                        'YPF', 'VISTA ENERGY', 'PAMPA', 'ROBECO', 'MEGATRENDS',
                        'REAL ESTATE', 'AMUNDI GLOBAL',
                        'ISHARES VII PLC CORE', 'BERKSHIRE HATHAWAY',
                        'WELLINGTON ENDURING ASSETS', 'COBAS SELECTION FUND',
                        'META PLATFORMS', 'NVIDIA CORP', 'MERCADOLIBRE', 'MICROSOFT CORP',
                        'NU HOLDINGS', 'TESLA', 'AMAZON COM INC', 'ALPHABET INC',
                        'GRID INFRASTRUCTURE', 'VANECK NLR',
                        'NOVO NORDISK']),
    ('Renta Fija',     []),
]

# Posiciones que siempre deben eliminarse (nombres incorrectos/duplicados del scraper)
# Tuplas (match_type, valor): 'eq' = exacto, 'ilike' = contiene (wildcard)
_POSITIONS_TO_DELETE = [
    ('eq',    'WELLINGTON ENDURING ASSETS'),           # truncado sin "FUND"; exacto para no borrar CLASS N/D
    ('ilike', 'CLEAN EDGE SMART GRID INFRASTRUCTURE'), # nombre largo incorrecto de GRID INFRASTRUCTURE ETF
    ('eq',    'VANECK ETF'),                           # nombre genérico, el correcto es VANECK NLR ETF
    ('eq',    'SPDR GOLD ETF'),                        # nombre viejo, el correcto es SPDR GOLD TR GOLD SHS
]

def _categorize(name):
    n = name.upper()
    for cat, keywords in _CATEGORIES:
        for kw in keywords:
            if kw in n:
                return cat
    return 'Renta Fija'

def _parse_positions(account_id, text):
    """Returns list of position dicts with value_usd, pct_cart, category. gl_usd/gl_pct=None."""
    lines = [l.strip() for l in text.split('\n')]
    # "Showing X records" limita la cantidad real de posiciones
    m_showing = re.search(r'Showing (\d+) records', text)
    showing = int(m_showing.group(1)) if m_showing else None

    header_end = None
    for i, line in enumerate(lines):
        if line == 'Maturity Date':
            header_end = i + 1
            break
    if header_end is None:
        return []
    remaining = lines[header_end:]
    names = []
    values_start = None
    for i, line in enumerate(remaining):
        if not line:
            continue
        # _SIGNED_RE captura positivos Y negativos — los negativos son posiciones short
        # cuyo valor aparece justo después de los nombres; detener aquí la sección de nombres
        if _SIGNED_RE.match(line):
            values_start = i
            break
        if not line.startswith('Disclaimers') and not line.startswith('INVESTMENT'):
            names.append(line)
    if values_start is None or not names:
        return []
    # Limitar a "Showing X" para no incluir texto basura capturado como nombre
    if showing and len(names) > showing:
        names = names[:showing]
    n_records = len(names)
    val_lines = [
        l for l in remaining[values_start:]
        if l and not l.startswith('Disclaimers') and not l.startswith('INVESTMENT') and l != 'Guide Me'
    ]
    positions = []
    i = 0
    sec_idx = 0
    while i < len(val_lines) and sec_idx < n_records:
        line = val_lines[i]
        if not _SIGNED_RE.match(line):
            i += 1
            continue
        market_value = float(line.replace(',', ''))
        i += 1
        pct = None
        if i < len(val_lines) and _SIGNED_RE.match(val_lines[i]):
            pct = float(val_lines[i])
            i += 1
        if i < len(val_lines) and _ISIN_RE.match(val_lines[i]):
            i += 1
        if i < len(val_lines) and _DEC4_RE.match(val_lines[i].replace(',', '')):
            i += 1
        if i < len(val_lines) and _DEC4_RE.match(val_lines[i].replace(',', '')):
            i += 1
        if i < len(val_lines) and not _MONEY_RE.match(val_lines[i]) and not _MATURITY_RE.match(val_lines[i]):
            i += 1
        if i < len(val_lines) and not _MONEY_RE.match(val_lines[i]) and not _MATURITY_RE.match(val_lines[i]):
            i += 1
        if i < len(val_lines) and _MATURITY_RE.match(val_lines[i]):
            i += 1
        name = names[sec_idx]
        positions.append({
            'account':   account_id,
            'name':      name,
            'category':  _categorize(name),
            'pct_cart':  pct,
            'value_usd': market_value,
            'gl_usd':    None,
            'gl_pct':    None,
        })
        sec_idx += 1
    return positions

def _parse_unrealized(text):
    """Returns dict {name -> {'gl_usd': float, 'gl_pct': float}}."""
    lines = [l.strip() for l in text.split('\n')]
    n_records = None
    for line in lines:
        m = re.search(r'Showing\s+(\d+)\s+records', line)
        if m:
            n_records = int(m.group(1))
            break
    if not n_records:
        return {}
    header_end = None
    for i, line in enumerate(lines):
        if line == 'Current Yield':
            header_end = i + 1
            break
    if header_end is None:
        return {}
    remaining = lines[header_end:]
    _UI_NOISE = {'Provide', 'Provide Cost Basis', 'N/A', 'Guide Me', 'Actions'}
    names = []
    values_start = None
    i = 0
    while i < len(remaining) and len(names) < n_records:
        line = remaining[i]
        if not line or line == 'Total' or line in _UI_NOISE:
            i += 1
            continue
        if line.startswith('Disclaimers') or line.startswith('INVESTMENT'):
            break
        if not _SIGNED_RE.match(line):
            names.append(line)
            i += 1
            while i < len(remaining) and not remaining[i]:
                i += 1
            if i < len(remaining) and _SIGNED_RE.match(remaining[i]):
                i += 1
        else:
            values_start = i
            break
    if len(names) == n_records and values_start is None:
        values_start = i
    if values_start is None or len(names) != n_records:
        return {}
    val_lines = [
        l for l in remaining[values_start:]
        if l and l != 'Total'
        and not l.startswith('Disclaimers')
        and not l.startswith('INVESTMENT')
        and l != 'Guide Me'
    ]
    result = {}
    for sec_idx in range(n_records):
        base = sec_idx * 5
        if base + 2 >= len(val_lines):
            break
        try:
            gl_usd = float(val_lines[base + 1].replace(',', ''))
            gl_pct = float(val_lines[base + 2].replace(',', ''))
            result[names[sec_idx]] = {'gl_usd': gl_usd, 'gl_pct': gl_pct}
        except (ValueError, IndexError):
            pass
    return result


# ── Supabase upsert ───────────────────────────────────────────────────────────

def upsert_to_supabase(results):
    import urllib.request
    import urllib.error
    import urllib.parse
    import ssl

    ssl_ctx = ssl.create_default_context()
    try:
        ssl_ctx.load_verify_locations('/etc/ssl/cert.pem')
    except Exception:
        pass

    _V2_ONLY = {'positions_text', 'unrealized_text', 'realized_text', 'history'}
    # Solo subir cuentas con datos reales — no sobreescribir con nulls si el scraper falló
    real_results = [r for r in results if r.get('ending_value') is not None]
    if not real_results:
        print("⚠  Supabase: nada que subir (todas las cuentas tienen ending_value=None)")
        return
    # No sobreescribir name si el scraper solo tiene el ID (name == account)
    _SKIP_NAME = {r['account'] for r in real_results if r.get('name') == r.get('account')}
    if _SKIP_NAME:
        print(f"⚠  Omitiendo 'name' para {len(_SKIP_NAME)} cuentas sin nombre real: {_SKIP_NAME}")
    tracker_rows = [
        {k: v for k, v in r.items()
         if k not in _V2_ONLY and not (k == 'name' and r['account'] in _SKIP_NAME)}
        for r in real_results
    ]

    headers = {
        'apikey':        SB_KEY,
        'Authorization': f'Bearer {SB_KEY}',
        'Content-Type':  'application/json',
        'Prefer':        'resolution=merge-duplicates',
    }

    # performance_tracker
    url     = f'{SB_URL}/rest/v1/performance_tracker'
    payload = json.dumps(tracker_rows, default=str).encode('utf-8')
    req     = urllib.request.Request(url, data=payload, headers=headers, method='POST')
    try:
        with urllib.request.urlopen(req, timeout=30, context=ssl_ctx) as resp:
            print(f"✅ Datos subidos a Supabase ({len(results)} cuentas)")
    except urllib.error.HTTPError as e:
        print(f"⚠  Supabase error {e.code}: {e.read().decode()[:300]}")
    except Exception as e:
        print(f"⚠  No se pudo subir a Supabase: {e}")

    # performance_history
    history_rows = []
    for r in real_results:
        for h in (r.get('history') or []):
            history_rows.append({
                'account':          r['account'],
                'period_start':     h['period_start'],
                'period_end':       h['period_end'],
                'ending_value':     h['ending_value'],
                'net_contribution': h['net_contribution'],
                'pct':              h['pct'],
                'partial':          h['partial'],
            })
    if history_rows:
        url2     = f'{SB_URL}/rest/v1/performance_history'
        payload2 = json.dumps(history_rows, default=str).encode('utf-8')
        req2     = urllib.request.Request(
            url2, data=payload2,
            headers={**headers, 'Prefer': 'resolution=merge-duplicates'},
            method='POST'
        )
        try:
            with urllib.request.urlopen(req2, timeout=30, context=ssl_ctx) as resp:
                print(f"✅ History upserted ({len(history_rows)} rows)")
        except urllib.error.HTTPError as e:
            print(f"⚠ History error {e.code}: {e.read().decode()[:200]}")

    # performance_positions (con G/L)
    position_rows = []
    gl_matched = 0
    gl_accounts = 0
    pos_errors = []
    for r in real_results:
        acct = r['account']
        positions = _parse_positions(acct, r.get('positions_text') or '')
        if not positions:
            pos_errors.append(acct)
            continue
        gl_map = _parse_unrealized(r.get('unrealized_text') or '')
        if gl_map:
            gl_accounts += 1
            for pos in positions:
                gl = gl_map.get(pos['name'])
                if gl:
                    pos['gl_usd'] = gl['gl_usd']
                    pos['gl_pct'] = gl['gl_pct']
                    gl_matched += 1
        position_rows.extend(positions)

    print(f"  Positions parsed : {len(position_rows)} rows, {len(real_results) - len(pos_errors)} cuentas")
    print(f"  GL data merged   : {gl_matched} filas en {gl_accounts} cuentas")
    if pos_errors:
        print(f"  ⚠  Positions sin datos: {pos_errors}")

    if position_rows:
        url3     = f'{SB_URL}/rest/v1/performance_positions?on_conflict=account,name'
        payload3 = json.dumps(position_rows, default=str).encode('utf-8')
        req3     = urllib.request.Request(
            url3, data=payload3,
            headers={**headers, 'Prefer': 'resolution=merge-duplicates'},
            method='POST'
        )
        try:
            with urllib.request.urlopen(req3, timeout=30, context=ssl_ctx) as resp:
                print(f"✅ Positions upserted ({len(position_rows)} rows)")
        except urllib.error.HTTPError as e:
            print(f"⚠ Positions error {e.code}: {e.read().decode()[:200]}")

        # Limpiar posiciones con nombres incorrectos/duplicados
        deleted_total = 0
        for match_type, bad_name in _POSITIONS_TO_DELETE:
            if match_type == 'eq':
                del_url = f"{SB_URL}/rest/v1/performance_positions?name=eq.{urllib.parse.quote(bad_name)}"
            else:
                del_url = f"{SB_URL}/rest/v1/performance_positions?name=ilike.*{urllib.parse.quote(bad_name)}*"
            del_req = urllib.request.Request(
                del_url, headers={**headers, 'Prefer': 'return=representation'}, method='DELETE'
            )
            try:
                with urllib.request.urlopen(del_req, timeout=30, context=ssl_ctx) as resp:
                    deleted = json.loads(resp.read().decode())
                    if deleted:
                        deleted_total += len(deleted)
                        for d in deleted:
                            print(f"  🗑  Eliminado: [{d['account']}] {d['name']}")
            except urllib.error.HTTPError as e:
                print(f"⚠ Cleanup error ({bad_name[:40]}): {e.read().decode()[:200]}")
        if deleted_total:
            print(f"✅ Cleanup: {deleted_total} posición(es) incorrecta(s) eliminada(s)")


def _save(results):
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(results, f, indent=2, default=str)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main(test_mode=False):
    async with async_playwright() as pw:
        # Limpiar pestañas residuales
        try:
            browser_temp = await pw.chromium.connect_over_cdp("http://127.0.0.1:9222")
            for ctx in browser_temp.contexts:
                for page in ctx.pages[1:]:
                    await page.close()
            await browser_temp.disconnect()
        except Exception:
            pass
        await asyncio.sleep(1)

        browser = await pw.chromium.connect_over_cdp("http://127.0.0.1:9222")

        for ctx in browser.contexts:
            for page in ctx.pages:
                if page.url in ('about:blank', ''):
                    try:
                        await page.close()
                    except Exception:
                        pass
        await asyncio.sleep(1)

        context     = browser.contexts[0]
        search_page = next(
            (p for p in context.pages if 'netx360.com' in p.url),
            await context.new_page()
        )
        if 'netx360.com' not in search_page.url:
            await search_page.goto(
                "https://www2.netx360.com/plus/my-practice/details/albridge-performance-account",
                wait_until='domcontentloaded', timeout=30000
            )
        print(f"Search page: {search_page.url}")

        # ── Lista de cuentas dinámica ─────────────────────────────────────
        try:
            accounts = await get_accounts_from_page(search_page)
        except Exception as e:
            print(f"⚠ No se pudo obtener cuentas de la web: {e}")
            print("  Usando fallback Excel...")
            accounts = get_accounts_from_excel()

        if test_mode:
            accounts = accounts[:1]
            print(f"\n*** TEST MODE — solo {accounts[0][1]} ***\n")
        else:
            print(f"\n*** MODO COMPLETO — {len(accounts)} cuentas ***\n")

        # ── Retomar desde donde se dejó ───────────────────────────────────
        results  = []
        done_ids = set()
        if not test_mode and os.path.exists(OUTPUT_FILE):
            try:
                with open(OUTPUT_FILE) as f:
                    results = json.load(f)
                done_ids = {r['account'] for r in results}
                print(f"Resumiendo: {len(results)} ya procesadas")
            except Exception as e:
                print(f"No se pudo cargar avance anterior: {e}")

        count = 0
        for name, account_id, value in accounts:
            if account_id in done_ids:
                print(f"Skip {account_id}")
                continue
            try:
                result = await asyncio.wait_for(
                    scrape_account(context, search_page, account_id, name, value),
                    timeout=ACCT_TIMEOUT
                )
            except asyncio.TimeoutError:
                print(f"  ✗ TIMEOUT — {account_id}")
                result = empty_result(account_id, name, value)

            results.append(result)
            done_ids.add(account_id)
            count += 1

            if count % SAVE_EVERY == 0:
                _save(results)
                print(f"  [guardado — {len(results)} resultados]")
                upsert_to_supabase(results)
                print(f"  [sincronizado con Supabase — {len(results)} cuentas]")

        _save(results)
        print(f"\n{'='*60}")
        print(f"LISTO — {len(results)} cuentas → {OUTPUT_FILE}")
        print(f"{'='*60}")
        upsert_to_supabase(results)

        await browser.close()


if __name__ == '__main__':
    test_mode = '--all' not in sys.argv
    if test_mode:
        print("Para correr todas las cuentas: python3 netx360_scraper_v3.py --all\n")
    asyncio.run(main(test_mode=test_mode))
