// GET /api/sync-sheets — Daily cron: sync Google Sheets → Supabase stock_productos
import { createClient } from '@supabase/supabase-js';
import { google } from 'googleapis';

let _supabase = null;
function getSupabase() {
  if (!_supabase) {
    const url = process.env.NEXT_PUBLIC_SUPABASE_URL || process.env.SUPABASE_URL;
    const key = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
    if (!url || !key) throw new Error(`Missing Supabase env vars. URL=${!!url}, KEY=${!!key}`);
    _supabase = createClient(url, key);
  }
  return _supabase;
}

function getAuth() {
  const raw = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
  if (!raw) throw new Error('Missing GOOGLE_SERVICE_ACCOUNT_JSON env var');
  const creds = JSON.parse(raw);
  return new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
  });
}

function parseNum(val) {
  if (val == null || val === '' || val === '-') return 0;
  if (typeof val === 'number') return val;
  const cleaned = String(val).replace(/\./g, '').replace(',', '.').replace(/[^0-9.\-]/g, '');
  const n = parseFloat(cleaned);
  return isNaN(n) ? 0 : n;
}

function getCurrentMonthCol() {
  const now = new Date();
  return now.getMonth();
}

// ─── SYNC 1: Stock Sistema (nueva planilla — código + stock total sumando depósitos) ───
async function syncStockSistema(sheets, config, supabase) {
  if (!config.sheet_id_stock_sistema) return { updated: 0, skipped: 'No sheet_id_stock_sistema configured' };

  // 1. Read "Descripcion" sheet to get code → name mapping
  let descMap = {};
  try {
    const descRes = await sheets.spreadsheets.values.get({
      spreadsheetId: config.sheet_id_stock_sistema,
      range: `'${config.sheet_name_descripcion || 'Descripcion'}'`,
      valueRenderOption: 'UNFORMATTED_VALUE',
    });
    const descRows = descRes.data.values || [];
    // Row 0 = headers: CODIGO, AGRUPA, NOMBRE_COR, DETALLE
    for (let r = 1; r < descRows.length; r++) {
      const row = descRows[r];
      const codigo = String(row[0] || '').trim();
      const nombre = String(row[2] || row[3] || '').trim();
      if (codigo) descMap[codigo] = nombre;
    }
  } catch (e) {
    console.warn('Could not read Descripcion sheet:', e.message);
  }

  // 2. Read "Stock Sistema" sheet — pivot table: CODIGO | dep1 | dep2 | ... | TOTAL
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: config.sheet_id_stock_sistema,
    range: `'${config.sheet_name_stock_sistema || 'Stock Sistema'}'`,
    valueRenderOption: 'UNFORMATTED_VALUE',
  });

  const rows = res.data.values || [];
  if (rows.length < 3) return { updated: 0, skipped: 'Not enough rows in Stock Sistema' };

  // Row 0: "SUM de STOCK" | "DEPOSITO" | ...
  // Row 1: "CODIGO" | dep_100 | dep_105 | ... | (empty col?) | "TOTAL"
  // Row 2+: codigo | qty_dep1 | qty_dep2 | ... | total
  const headerRow = rows[1] || [];

  // Find TOTAL column and deposit columns (numeric headers = deposit IDs)
  let totalColIdx = -1;
  const depositCols = [];
  for (let i = 0; i < headerRow.length; i++) {
    const h = String(headerRow[i] || '').toUpperCase().trim();
    if (h === 'TOTAL' || h === 'TOTAL GENERAL') {
      totalColIdx = i;
    } else if (i > 0 && !isNaN(Number(headerRow[i]))) {
      depositCols.push(i);
    }
  }

  console.log(`Stock Sistema: totalCol=${totalColIdx}, depositCols=${depositCols.length}, rows=${rows.length}`);

  const updates = [];
  for (let r = 2; r < rows.length; r++) {
    const row = rows[r];
    const codigoRaw = row[0];
    if (codigoRaw == null || codigoRaw === '' || String(codigoRaw).toUpperCase() === 'TOTAL') continue;
    const codigo = String(codigoRaw).trim();

    // Strategy: use TOTAL column if the row has data there, otherwise sum deposit columns
    let stockTotal = 0;
    if (totalColIdx >= 0 && row.length > totalColIdx && row[totalColIdx] != null) {
      stockTotal = parseNum(row[totalColIdx]);
    } else {
      // Sum all deposit columns
      for (const c of depositCols) {
        if (c < row.length && row[c] != null) {
          stockTotal += parseNum(row[c]);
        }
      }
    }

    if (stockTotal === 0 && !descMap[codigo]) continue;

    const nombre = descMap[codigo] || `Producto ${codigo}`;

    updates.push({
      codigo: codigo,
      nombre: nombre,
      stock_actual: stockTotal,
      fecha_sync: new Date().toISOString(),
    });
  }

  if (updates.length === 0) return { updated: 0, skipped: 'No products found in Stock Sistema' };

  // Build a map of codigo → stock_actual for fast lookup
  const stockMap = {};
  for (const u of updates) {
    stockMap[u.codigo] = u;
  }

  // Get ALL existing products from stock_productos (with marca assigned)
  const { data: existing } = await supabase
    .from('stock_productos')
    .select('id, codigo, marca')
    .neq('marca', 'Sin asignar');

  let updatedCount = 0;
  const matched = new Set();

  // Update stock_actual for existing products that match by codigo
  for (const prod of (existing || [])) {
    const upd = stockMap[prod.codigo];
    if (upd) {
      matched.add(prod.codigo);
      const { error } = await supabase
        .from('stock_productos')
        .update({ stock_actual: upd.stock_actual, fecha_sync: upd.fecha_sync })
        .eq('id', prod.id);
      if (!error) updatedCount++;
    }
  }

  // Don't insert "Sin asignar" products — only update existing ones with marca
  const notMatched = updates.length - matched.size;

  return { updated: updatedCount, matched_codes: matched.size, not_matched: notMatched, total_codes: updates.length };
}

// ─── SYNC 2: Ventas/Stock/Cobertura (planilla original) ───
async function syncStocks(sheets, config, supabase) {
  if (!config.sheet_id_stocks) return { updated: 0, skipped: 'No sheet_id_stocks configured' };

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: config.sheet_id_stocks,
    range: `'${config.sheet_name_stocks || 'Ventas stock cierre cobertura'}'`,
    valueRenderOption: 'UNFORMATTED_VALUE',
  });

  const rows = res.data.values || [];
  if (rows.length < 3) return { updated: 0, skipped: 'Not enough rows' };

  const headerRow = rows[0] || [];
  const monthRow = rows[1] || [];

  let stocksStartCol = -1, coberturaStartCol = -1, ventasStartCol = -1;
  for (let i = 0; i < headerRow.length; i++) {
    const h = String(headerRow[i] || '').toUpperCase().trim();
    if (h.includes('VENTAS') && ventasStartCol === -1) ventasStartCol = i;
    if (h.includes('STOCK') && h.includes('CIERRE')) stocksStartCol = i;
    if (h.includes('COBERTURA')) coberturaStartCol = i;
  }

  const currentMonth = getCurrentMonthCol();
  const monthNames = ['ENE','FEB','MAR','ABR','MAY','JUN','JUL','AGO','SEP','OCT','NOV','DIC'];
  const curMonthName = monthNames[currentMonth];

  function findMonthCol(startCol) {
    if (startCol < 0) return -1;
    for (let i = startCol; i < Math.min(startCol + 15, monthRow.length); i++) {
      const m = String(monthRow[i] || '').toUpperCase().trim().substring(0, 3);
      if (m === curMonthName) return i;
    }
    return startCol + 1;
  }

  const ventasCol = findMonthCol(ventasStartCol);
  const stockCol = findMonthCol(stocksStartCol);
  const coberturaCol = findMonthCol(coberturaStartCol);

  const productos = [];
  for (let r = 2; r < rows.length; r++) {
    const row = rows[r];
    const marca = String(row[0] || '').trim();
    const nombre = String(row[1] || '').trim();
    if (!marca || !nombre || marca === 'TOTAL' || marca === 'Total') continue;

    const codigo = nombre.substring(0, 30).replace(/\s+/g, '_').toUpperCase();

    productos.push({
      codigo: codigo,
      nombre: nombre,
      marca: marca,
      stock_actual: parseNum(row[stockCol]),
      venta_mensual_avg: parseNum(row[ventasCol]),
      cobertura_meses: parseNum(row[coberturaCol]),
      fecha_sync: new Date().toISOString(),
    });
  }

  if (productos.length === 0) return { updated: 0, skipped: 'No products found' };

  const { error } = await supabase.from('stock_productos').upsert(
    productos,
    { onConflict: 'codigo,marca' }
  );

  if (error) throw new Error(`Upsert stocks error: ${error.message}`);
  return { updated: productos.length };
}

// ─── SYNC 3: Forecast ───
async function syncForecast(sheets, config, supabase) {
  if (!config.sheet_id_forecast) return { updated: 0, skipped: 'No sheet_id_forecast configured' };

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: config.sheet_id_forecast,
    range: `'${config.sheet_name_forecast || 'Resumen Gral.'}'`,
    valueRenderOption: 'UNFORMATTED_VALUE',
  });

  const rows = res.data.values || [];
  if (rows.length < 2) return { updated: 0, skipped: 'Not enough rows' };

  const headers = (rows[0] || []).map(h => String(h || '').toUpperCase().trim());
  const colIdx = (name) => headers.findIndex(h => h.includes(name));

  const iSeg = colIdx('SEGMENTO');
  const iPolo = colIdx('POLO');
  const iMarca = colIdx('MARCA');
  const iCodigo = colIdx('CODIGO');
  const iNombre = colIdx('NOMBRE');

  const nextMonth = (getCurrentMonthCol() + 1) % 12;
  const monthNames2 = ['ENE','FEB','MAR','ABR','MAY','JUN','JUL','AGO','SEP','OCT','NOV','DIC'];
  const nextMonthName = monthNames2[nextMonth];

  let forecastCol = -1;
  for (let i = 0; i < headers.length; i++) {
    const h = headers[i];
    if (h.includes(nextMonthName) && (h.includes('QTY') || h.includes('UNI') || h.includes('CANT'))) {
      forecastCol = i;
      break;
    }
  }
  if (forecastCol === -1) {
    for (let i = 0; i < headers.length; i++) {
      if (String(headers[i]).includes(nextMonthName)) { forecastCol = i; break; }
    }
  }

  const updates = [];
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r];
    const marca = String(row[iMarca] || '').trim();
    const codigo = String(row[iCodigo] || '').trim();
    const nombre = String(row[iNombre] || '').trim();
    if (!marca || !codigo) continue;

    const forecastVal = forecastCol >= 0 ? parseNum(row[forecastCol]) : 0;

    updates.push({
      codigo: codigo,
      nombre: nombre || codigo,
      marca: marca,
      segmento: iSeg >= 0 ? String(row[iSeg] || '').trim() : null,
      polo: iPolo >= 0 ? String(row[iPolo] || '').trim() : null,
      forecast_proximo_mes: forecastVal,
      fecha_sync: new Date().toISOString(),
    });
  }

  if (updates.length === 0) return { updated: 0, skipped: 'No forecast products found' };

  const { error } = await supabase.from('stock_productos').upsert(
    updates,
    { onConflict: 'codigo,marca', ignoreDuplicates: false }
  );

  if (error) throw new Error(`Upsert forecast error: ${error.message}`);
  return { updated: updates.length };
}

// ─── HANDLER ───
export default async function handler(req, res) {
  let authorized = false;
  const authHeader = req.headers.authorization || '';

  if (authHeader === `Bearer ${process.env.CRON_SECRET}`) authorized = true;
  if (!authorized && req.headers['x-api-key'] === process.env.INTERNAL_API_KEY) authorized = true;
  if (!authorized && authHeader.startsWith('Bearer ')) {
    try {
      const sb = getSupabase();
      const token = authHeader.replace('Bearer ', '');
      const { data: { user }, error } = await sb.auth.getUser(token);
      if (user && !error) authorized = true;
    } catch (_) {}
  }

  if (!authorized) return res.status(401).json({ error: 'Unauthorized' });

  try {
    const supabase = getSupabase();

    const { data: configArr } = await supabase.from('config_sheets').select('*').eq('id', 1);
    const config = configArr?.[0];
    if (!config || !config.sync_enabled) {
      return res.status(200).json({ message: 'Sync disabled or not configured' });
    }

    const auth = getAuth();
    const sheets = google.sheets({ version: 'v4', auth });

    // Step 1: Sync stocks + forecast in parallel (these CREATE products with marca)
    const [stockResult, forecastResult] = await Promise.allSettled([
      syncStocks(sheets, config, supabase),
      syncForecast(sheets, config, supabase),
    ]);

    const stockRes = stockResult.status === 'fulfilled' ? stockResult.value : { error: stockResult.reason?.message };
    const forecastRes = forecastResult.status === 'fulfilled' ? forecastResult.value : { error: forecastResult.reason?.message };

    // Step 2: THEN sync Stock Sistema (updates stock_actual on existing products by codigo)
    let stockSistemaRes = { skipped: 'Not configured' };
    try {
      stockSistemaRes = await syncStockSistema(sheets, config, supabase);
    } catch (e) {
      stockSistemaRes = { error: e.message };
    }

    await supabase.rpc('recalculate_cobertura');

    const totalUpdated = (stockSistemaRes.updated || 0) + (stockSistemaRes.inserted || 0) + (stockRes.updated || 0) + (forecastRes.updated || 0);
    await supabase.from('stock_sync_log').insert({
      productos_actualizados: totalUpdated,
      errores: [stockSistemaRes.error, stockRes.error, forecastRes.error].filter(Boolean).join('; ') || null,
    });

    await supabase.from('config_sheets').update({ last_sync: new Date().toISOString() }).eq('id', 1);

    return res.status(200).json({
      message: 'Sync completed',
      stock_sistema: stockSistemaRes,
      stocks: stockRes,
      forecast: forecastRes,
    });
  } catch (err) {
    console.error('Sync sheets error:', err);
    return res.status(500).json({ error: err.message });
  }
}
