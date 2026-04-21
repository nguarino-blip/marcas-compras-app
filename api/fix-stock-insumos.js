import { createClient } from '@supabase/supabase-js';
import { google } from 'googleapis';

function getSupabase() {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL || process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
  if (!url || !key) throw new Error('Missing Supabase env vars');
  return createClient(url, key);
}

function getAuth() {
  const raw = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
  if (!raw) throw new Error('Missing GOOGLE_SERVICE_ACCOUNT_JSON');
  return new google.auth.GoogleAuth({ credentials: JSON.parse(raw), scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] });
}

function parseNum(val) {
  if (val == null || val === '' || val === '-') return 0;
  if (typeof val === 'number') return val;
  const cleaned = String(val).replace(/\./g, '').replace(',', '.').replace(/[^0-9.\-]/g, '');
  return parseFloat(cleaned) || 0;
}

const normCode = (c) => {
  let s = String(c || '').trim();
  s = s.replace(/\.0+$/, '');
  s = s.replace(/^0+(?=\d)/, '');
  return s;
};

export default async function handler(req, res) {
  const diag = { steps: [], errors: [] };
  try {
    const supabase = getSupabase();
    const auth = getAuth();
    const sheets = google.sheets({ version: 'v4', auth });
    const { data: cfgRows } = await supabase.from('config_sheets').select('*').limit(1);
    const config = cfgRows && cfgRows[0] ? cfgRows[0] : {};
    const sheetId = config.sheet_id_stock_sistema;
    if (!sheetId) return res.status(400).json({ error: 'No sheet_id_stock_sistema in config' });
    diag.steps.push('Config loaded');

    const meta = await sheets.spreadsheets.get({ spreadsheetId: sheetId, fields: 'sheets.properties.title' });
    const allTabs = (meta.data.sheets || []).map(s => s.properties.title);
    diag.tabs = allTabs;
    const tryNames = ['STOCK', 'Stock al d\u00eda', 'Hoja 1', 'Hoja1', ...allTabs];
    let rows = [], usedSheet = '';
    for (const sn of tryNames) {
      try {
        const r = await sheets.spreadsheets.values.get({ spreadsheetId: sheetId, range: "'" + sn + "'", valueRenderOption: 'UNFORMATTED_VALUE' });
        rows = r.data.values || [];
        if (rows.length >= 3) { usedSheet = sn; break; }
      } catch (e) { /* skip */ }
    }
    if (rows.length < 3) return res.status(400).json({ error: 'No stock sheet found', tabs: allTabs });
    diag.steps.push('Sheet=' + usedSheet + ' rows=' + rows.length);
    diag.row0 = (rows[0] || []).slice(0, 6);
    diag.row1 = (rows[1] || []).slice(0, 8);
    diag.row2 = (rows[2] || []).slice(0, 6);

    let headerRow, dataStartRow;
    const r0 = String((rows[0] || [])[0] || '').toUpperCase().trim();
    if (r0 === 'CODIGO' || r0 === 'C\u00d3DIGO') { headerRow = rows[0]; dataStartRow = 1; }
    else { headerRow = rows[1] || []; dataStartRow = 2; }

    const depositCols = [];
    let totalColIdx = -1, stockColIdx = -1;
    for (let i = 0; i < headerRow.length; i++) {
      const h = String(headerRow[i] || '').toUpperCase().trim();
      if (h === 'TOTAL' || h === 'TOTAL GENERAL') totalColIdx = i;
      else if (h === 'STOCK' || h === 'CANTIDAD') stockColIdx = i;
      else if (i > 0 && !isNaN(Number(headerRow[i]))) depositCols.push(i);
    }
    diag.steps.push(depositCols.length + ' depots, totalCol=' + totalColIdx + ', stockCol=' + stockColIdx);
    diag.headerSample = headerRow.slice(0, 10);

    const stockMap = {};
    for (let r = dataStartRow; r < rows.length; r++) {
      const row = rows[r];
      const cr = row[0];
      if (cr == null || cr === '' || String(cr).toUpperCase() === 'TOTAL') continue;
      const codigo = String(cr).trim();
      let st = 0;
      if (totalColIdx >= 0 && row[totalColIdx] != null) st = parseNum(row[totalColIdx]);
      else if (stockColIdx >= 0 && row[stockColIdx] != null) st = parseNum(row[stockColIdx]);
      else if (depositCols.length > 0) { for (const c of depositCols) { if (c < row.length && row[c] != null) st += parseNum(row[c]); } }
      else if (row.length > 1 && row[1] != null) st = parseNum(row[1]);
      if (!stockMap[codigo] || st > stockMap[codigo]) stockMap[codigo] = st;
    }

    const stockMapNorm = {};
    for (const [k, v] of Object.entries(stockMap)) {
      const nk = normCode(k);
      if (!stockMapNorm[nk] || v > stockMapNorm[nk]) stockMapNorm[nk] = v;
    }
    diag.steps.push('StockMap: ' + Object.keys(stockMap).length + ' raw, ' + Object.keys(stockMapNorm).length + ' norm');
    diag.stockFirst20 = Object.keys(stockMap).slice(0, 20);
    diag.stockSamples = Object.entries(stockMap).slice(0, 5).map(([k, v]) => ({ k, v, t: typeof k, n: normCode(k) }));

    const dbg = ['13897', '11400', '26128'];
    diag.debug = {};
    for (const dc of dbg) {
      const rawKeys = Object.keys(stockMap).filter(k => normCode(k) === dc);
      diag.debug[dc] = { rawVal: stockMap[dc], normVal: stockMapNorm[dc], matchingRawKeys: rawKeys };
    }

    const { data: existingInsumos, error: fetchErr } = await supabase.from('stock_insumos').select('codigo');
    if (fetchErr) return res.status(500).json({ error: fetchErr.message });
    const existingCodes = (existingInsumos || []).map(i => i.codigo);
    diag.steps.push('BOM codes: ' + existingCodes.length);
    diag.bomFirst20 = existingCodes.slice(0, 20);

    const normToBomCode = {};
    for (const c of existingCodes) { normToBomCode[normCode(c)] = c; }
    const existingNormSet = new Set(Object.keys(normToBomCode));

    for (const dc of dbg) {
      diag.debug[dc].inBOM = existingCodes.includes(dc);
      diag.debug[dc].inBOMnorm = existingNormSet.has(dc);
      diag.debug[dc].bomMatchKeys = existingCodes.filter(c => normCode(c) === dc);
    }

    let updatedCount = 0, matchedCount = 0;
    const matchDetails = [];
    const normKeys = Object.keys(stockMapNorm);
    for (let i = 0; i < normKeys.length; i += 200) {
      const chunk = normKeys.slice(i, i + 200);
      const updates = chunk.filter(c => existingNormSet.has(c)).map(c => ({
        codigo: normToBomCode[c] || c,
        stock_fisico: stockMapNorm[c],
        stock_disponible: stockMapNorm[c],
        fecha_sync: new Date().toISOString(),
      }));
      matchedCount += updates.length;
      if (updates.length === 0) continue;
      if (matchDetails.length < 20) {
        for (const u of updates.slice(0, 20 - matchDetails.length)) matchDetails.push(u.codigo + '=' + u.stock_fisico);
      }
      const { error } = await supabase.from('stock_insumos').upsert(updates, { onConflict: 'codigo', ignoreDuplicates: false });
      if (!error) updatedCount += updates.length;
      else diag.errors.push(error.message);
    }

    const unmatchedBOM = existingCodes.filter(c => stockMapNorm[normCode(c)] === undefined);
    diag.matched = matchedCount;
    diag.updated = updatedCount;
    diag.matchedFirst20 = matchDetails;
    diag.unmatchedCount = unmatchedBOM.length;
    diag.unmatchedSample = unmatchedBOM.slice(0, 30);

    return res.status(200).json({ ok: true, matched: matchedCount, updated: updatedCount, totalStock: Object.keys(stockMap).length, totalBOM: existingCodes.length, diag });
  } catch (err) {
    diag.errors.push(err.message);
    return res.status(500).json({ error: err.message, diag });
  }
}
