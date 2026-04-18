// GET /api/sync-sheets — Daily cron: sync Google Sheets → Supabase stock_productos
import { createClient } from '@supabase/supabase-js';
import { google } from 'googleapis';

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

function getAuth() {
  // Service account credentials from env (JSON string)
  const creds = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON || '{}');
  return new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
  });
}

// Parse a number from sheet cell (handles Argentine format: 1.234,56)
function parseNum(val) {
  if (val == null || val === '' || val === '-') return 0;
  if (typeof val === 'number') return val;
  const cleaned = String(val).replace(/\./g, '').replace(',', '.').replace(/[^0-9.\-]/g, '');
  const n = parseFloat(cleaned);
  return isNaN(n) ? 0 : n;
}

// Get current month index (0=Jan) and build column mapping for stock sheet
function getCurrentMonthCol() {
  const now = new Date();
  return now.getMonth(); // 0-indexed
}

async function syncStocks(sheets, config) {
  if (!config.sheet_id_stocks) return { updated: 0, skipped: 'No sheet_id_stocks configured' };

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: config.sheet_id_stocks,
    range: `'${config.sheet_name_stocks || 'Ventas stock cierre cobertura'}'`,
    valueRenderOption: 'UNFORMATTED_VALUE',
  });

  const rows = res.data.values || [];
  if (rows.length < 3) return { updated: 0, skipped: 'Not enough rows' };

  // Find header rows — the sheet has a complex structure with 4 sections side by side
  // Row 0: section headers (VENTAS, STOCKS A CIERRE, COBERTURA, INGRESOS)
  // Row 1: month names
  // Row 2+: data rows with Marca in col 0, Producto in col 1
  const headerRow = rows[0] || [];
  const monthRow = rows[1] || [];

  // Find section start columns
  let stocksStartCol = -1, coberturaStartCol = -1, ventasStartCol = -1;
  for (let i = 0; i < headerRow.length; i++) {
    const h = String(headerRow[i] || '').toUpperCase().trim();
    if (h.includes('VENTAS') && ventasStartCol === -1) ventasStartCol = i;
    if (h.includes('STOCK') && h.includes('CIERRE')) stocksStartCol = i;
    if (h.includes('COBERTURA')) coberturaStartCol = i;
  }

  // Find the current month column in each section
  const currentMonth = getCurrentMonthCol();
  const monthNames = ['ENE','FEB','MAR','ABR','MAY','JUN','JUL','AGO','SEP','OCT','NOV','DIC'];
  const curMonthName = monthNames[currentMonth];

  function findMonthCol(startCol) {
    if (startCol < 0) return -1;
    for (let i = startCol; i < Math.min(startCol + 15, monthRow.length); i++) {
      const m = String(monthRow[i] || '').toUpperCase().trim().substring(0, 3);
      if (m === curMonthName) return i;
    }
    // Fallback: use the last available column in the section
    return startCol + 1;
  }

  const ventasCol = findMonthCol(ventasStartCol);
  const stockCol = findMonthCol(stocksStartCol);
  const coberturaCol = findMonthCol(coberturaStartCol);

  // Parse data rows
  const productos = [];
  for (let r = 2; r < rows.length; r++) {
    const row = rows[r];
    const marca = String(row[0] || '').trim();
    const nombre = String(row[1] || '').trim();
    if (!marca || !nombre || marca === 'TOTAL' || marca === 'Total') continue;

    // Use marca as code if no separate code column, otherwise try col 1 for code
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

  // Upsert to Supabase
  const { error } = await supabase.from('stock_productos').upsert(
    productos,
    { onConflict: 'codigo,marca' }
  );

  if (error) throw new Error(`Upsert stocks error: ${error.message}`);
  return { updated: productos.length };
}

async function syncForecast(sheets, config) {
  if (!config.sheet_id_forecast) return { updated: 0, skipped: 'No sheet_id_forecast configured' };

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: config.sheet_id_forecast,
    range: `'${config.sheet_name_forecast || 'Resumen Gral.'}'`,
    valueRenderOption: 'UNFORMATTED_VALUE',
  });

  const rows = res.data.values || [];
  if (rows.length < 2) return { updated: 0, skipped: 'Not enough rows' };

  // Find headers — expected: SEGMENTO, POLO, MARCA, CODIGO_PRODUCTO, NOMBRE_PRODUCTO, then monthly columns
  const headers = (rows[0] || []).map(h => String(h || '').toUpperCase().trim());
  const colIdx = (name) => headers.findIndex(h => h.includes(name));

  const iSeg = colIdx('SEGMENTO');
  const iPolo = colIdx('POLO');
  const iMarca = colIdx('MARCA');
  const iCodigo = colIdx('CODIGO');
  const iNombre = colIdx('NOMBRE');

  // Find forecast columns for next month
  const nextMonth = (getCurrentMonthCol() + 1) % 12;
  const monthNames2 = ['ENE','FEB','MAR','ABR','MAY','JUN','JUL','AGO','SEP','OCT','NOV','DIC'];
  const nextMonthName = monthNames2[nextMonth];

  // Look for forecast quantity column (QTY or UNIDADES) for next month
  let forecastCol = -1;
  for (let i = 0; i < headers.length; i++) {
    const h = headers[i];
    if (h.includes(nextMonthName) && (h.includes('QTY') || h.includes('UNI') || h.includes('CANT'))) {
      forecastCol = i;
      break;
    }
  }
  // Fallback: just find next month name
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

  // Upsert — only update forecast fields, don't overwrite stock data
  const { error } = await supabase.from('stock_productos').upsert(
    updates,
    { onConflict: 'codigo,marca', ignoreDuplicates: false }
  );

  if (error) throw new Error(`Upsert forecast error: ${error.message}`);
  return { updated: updates.length };
}

export default async function handler(req, res) {
  // Auth: cron secret or internal API key
  if (req.headers.authorization !== `Bearer ${process.env.CRON_SECRET}`) {
    if (req.headers['x-api-key'] !== process.env.INTERNAL_API_KEY) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
  }

  try {
    // Get config
    const { data: configArr } = await supabase.from('config_sheets').select('*').eq('id', 1);
    const config = configArr?.[0];
    if (!config || !config.sync_enabled) {
      return res.status(200).json({ message: 'Sync disabled or not configured' });
    }

    const auth = getAuth();
    const sheets = google.sheets({ version: 'v4', auth });

    const [stockResult, forecastResult] = await Promise.allSettled([
      syncStocks(sheets, config),
      syncForecast(sheets, config),
    ]);

    const stockRes = stockResult.status === 'fulfilled' ? stockResult.value : { error: stockResult.reason?.message };
    const forecastRes = forecastResult.status === 'fulfilled' ? forecastResult.value : { error: forecastResult.reason?.message };

    // Update cobertura_meses for products that now have both stock and forecast/venta
    await supabase.rpc('recalculate_cobertura');

    // Log sync
    await supabase.from('stock_sync_log').insert({
      productos_actualizados: (stockRes.updated || 0) + (forecastRes.updated || 0),
      errores: [stockRes.error, forecastRes.error].filter(Boolean).join('; ') || null,
    });

    // Update last_sync
    await supabase.from('config_sheets').update({ last_sync: new Date().toISOString() }).eq('id', 1);

    return res.status(200).json({
      message: 'Sync completed',
      stocks: stockRes,
      forecast: forecastRes,
    });
  } catch (err) {
    console.error('Sync sheets error:', err);
    return res.status(500).json({ error: err.message });
  }
}
