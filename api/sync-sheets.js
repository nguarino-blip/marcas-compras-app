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

  // Parse all products from the sheet
  const sheetProds = [];
  for (let r = 2; r < rows.length; r++) {
    const row = rows[r];
    const marca = String(row[0] || '').trim();
    const nombre = String(row[1] || '').trim();
    if (!marca || !nombre || marca === 'TOTAL' || marca === 'Total') continue;

    sheetProds.push({
      nombre,
      marca,
      stock_actual: parseNum(row[stockCol]),
      venta_mensual_avg: parseNum(row[ventasCol]),
      cobertura_meses: parseNum(row[coberturaCol]),
      fecha_sync: new Date().toISOString(),
    });
  }

  if (sheetProds.length === 0) return { updated: 0, skipped: 'No products found' };

  // ── KEY FIX: Match by marca+nombre against existing records (which have real codes from forecast/BOM) ──
  const { data: existing } = await supabase
    .from('stock_productos')
    .select('id, codigo, nombre, marca')
    .range(0, 9999);

  // Build lookup: "MARCA||NOMBRE_UPPER" → existing record
  const existByKey = {};
  const existByNombrePartial = {};
  for (const ex of (existing || [])) {
    const key = (ex.marca || '').toUpperCase() + '||' + (ex.nombre || '').toUpperCase().replace(/\s+/g, ' ').trim();
    existByKey[key] = ex;
    // Also index by just nombre (uppercase, trimmed) for partial matching
    const nKey = (ex.nombre || '').toUpperCase().replace(/\s+/g, ' ').trim();
    if (nKey && !existByNombrePartial[nKey]) existByNombrePartial[nKey] = ex;
  }

  let updatedCount = 0;
  let insertedCount = 0;
  const updateBatch = [];
  const insertBatch = [];

  for (const sp of sheetProds) {
    const key = sp.marca.toUpperCase() + '||' + sp.nombre.toUpperCase().replace(/\s+/g, ' ').trim();
    let match = existByKey[key];

    // Fallback: try nombre-only match
    if (!match) {
      const nKey = sp.nombre.toUpperCase().replace(/\s+/g, ' ').trim();
      match = existByNombrePartial[nKey];
    }

    // Fallback: partial nombre match (one contains the other)
    if (!match) {
      const spUp = sp.nombre.toUpperCase();
      for (const ex of (existing || [])) {
        const exUp = (ex.nombre || '').toUpperCase();
        if (exUp && spUp && (exUp.includes(spUp) || spUp.includes(exUp))) {
          // Also check marca matches
          if ((ex.marca || '').toUpperCase() === sp.marca.toUpperCase()) {
            match = ex;
            break;
          }
        }
      }
    }

    if (match) {
      // Update existing record — preserves its real codigo
      updateBatch.push({
        id: match.id,
        stock_actual: sp.stock_actual,
        venta_mensual_avg: sp.venta_mensual_avg,
        cobertura_meses: sp.cobertura_meses,
        fecha_sync: sp.fecha_sync,
      });
    } else {
      // No existing record — insert with synthetic code (fallback)
      const codigo = sp.nombre.substring(0, 30).replace(/\s+/g, '_').toUpperCase();
      insertBatch.push({ codigo, ...sp });
    }
  }

  // Batch update matched records (preserving real codes)
  for (const upd of updateBatch) {
    const { id, ...fields } = upd;
    const { error } = await supabase.from('stock_productos').update(fields).eq('id', id);
    if (!error) updatedCount++;
  }

  // Insert unmatched as new (with synthetic code fallback)
  if (insertBatch.length > 0) {
    const { error } = await supabase.from('stock_productos').upsert(insertBatch, { onConflict: 'codigo,marca' });
    if (!error) insertedCount = insertBatch.length;
    else console.error('Insert unmatched stocks error:', error.message);
  }

  return { updated: updatedCount, inserted: insertedCount, total_sheet: sheetProds.length };
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

// ─── SYNC 4: BOM (BASE BRUTA — producto → insumos) ───
async function syncBOM(sheets, config, supabase) {
  if (!config.sheet_id_bom) return { updated: 0, skipped: 'No sheet_id_bom configured' };

  // Try configured sheet name, then fallbacks
  const sheetNames = [
    config.sheet_name_bom,
    'BASE BRUTA',
    'Nueva base bruta',
    'Base Bruta',
  ].filter(Boolean);

  let rows = [];
  let usedSheet = '';
  for (const sn of sheetNames) {
    try {
      const res = await sheets.spreadsheets.values.get({
        spreadsheetId: config.sheet_id_bom,
        range: `'${sn}'`,
        valueRenderOption: 'UNFORMATTED_VALUE',
      });
      rows = res.data.values || [];
      if (rows.length >= 2) { usedSheet = sn; break; }
    } catch (e) {
      console.warn(`BOM sheet '${sn}' not found, trying next...`);
    }
  }
  if (rows.length < 2) return { updated: 0, skipped: 'BOM sheet not found (tried: ' + sheetNames.join(', ') + ')' };
  console.log(`BOM: using sheet '${usedSheet}' with ${rows.length} rows`);

  // Row 0 = headers (normalize: spaces→underscores, remove accents for robust matching)
  const headers = (rows[0] || []).map(h => String(h || '').toUpperCase().trim());
  const norm = s => s.replace(/\s+/g, '_').normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  const headersNorm = headers.map(norm);
  const col = (name) => {
    const nn = norm(name);
    // Try normalized includes first
    let idx = headersNorm.findIndex(h => h.includes(nn));
    if (idx >= 0) return idx;
    // Fallback: exact match on original
    idx = headers.indexOf(name);
    return idx;
  };

  const iCodPrincipal = col('CODIGO_PRINCIPAL');
  const iNivel = col('NIVEL_JERARQUIA') >= 0 ? col('NIVEL_JERARQUIA') : col('NIVEL');
  const iCategoria = col('CATEGORIA_NOMBRE') >= 0 ? col('CATEGORIA_NOMBRE') : col('CATEGORIA');
  // CODIGO: find column that is exactly "CODIGO" (not CODIGO_PRINCIPAL)
  const iCodigo = (() => {
    let idx = headers.indexOf('CODIGO');
    if (idx >= 0) return idx;
    idx = headersNorm.indexOf('CODIGO');
    if (idx >= 0) return idx;
    // Find column named CODIGO that is NOT CODIGO_PRINCIPAL
    for (let i = 0; i < headers.length; i++) {
      const h = headersNorm[i];
      if (h === 'CODIGO' || h === 'CODIGO_INSUMO' || h === 'COD_INSUMO') return i;
    }
    return -1;
  })();
  const iDetalle = col('DETALLE');
  const iCantFormula = col('CANTIDAD_FORMULA') >= 0 ? col('CANTIDAD_FORMULA') : col('CANTIDAD');
  const iTipoInsumo = col('TIPO_INSUMO');
  const iStockFisico = col('STOCK_FISICO') >= 0 ? col('STOCK_FISICO') : col('STOCK');
  const iDisponible = col('DISPONIBLE');
  const iGrupo = col('GRUPO_PRODUCTO') >= 0 ? col('GRUPO_PRODUCTO') : col('GRUPO');

  console.log('BOM headers found:', JSON.stringify(headers));
  console.log('BOM column indices:', JSON.stringify({ iCodPrincipal, iNivel, iCategoria, iCodigo, iDetalle, iCantFormula, iTipoInsumo, iStockFisico, iDisponible, iGrupo }));

  const bomItems = [];
  const insumoStock = {};

  for (let r = 1; r < rows.length; r++) {
    const row = rows[r];
    const codPrincipal = String(row[iCodPrincipal] || '').trim();
    const nivel = parseNum(row[iNivel]);
    const categoria = String(row[iCategoria] || '').trim();
    const codigo = String(row[iCodigo] || '').trim();
    const detalle = String(row[iDetalle] || '').trim();
    const cantFormula = iCantFormula >= 0 ? parseNum(row[iCantFormula]) : 1;
    const tipoInsumo = iTipoInsumo >= 0 ? String(row[iTipoInsumo] || '').trim() : '';
    const stockFisico = iStockFisico >= 0 ? parseNum(row[iStockFisico]) : 0;
    const disponible = iDisponible >= 0 ? parseNum(row[iDisponible]) : stockFisico;
    const nombrePrincipal = iGrupo >= 0 ? String(row[iGrupo] || '').trim() : '';

    if (!codPrincipal || !codigo) continue;

    // Detect envase: DETALLE contains ENVASE or FRASCO (but not CAJA, not ESENCIA, not TAPA, not COLLAR, not VALVULA)
    const detUp = detalle.toUpperCase();
    const esEnvase = (detUp.includes('ENVASE') || detUp.includes('FRASCO')) &&
                     !detUp.includes('CAJA') && !detUp.includes('ESENCIA');

    if (nivel > 0) {
      bomItems.push({
        codigo_principal: codPrincipal,
        nombre_principal: nombrePrincipal,
        nivel: nivel,
        categoria: categoria,
        codigo_insumo: codigo,
        detalle_insumo: detalle.substring(0, 200),
        cantidad_formula: cantFormula || 1,
        tipo_insumo: tipoInsumo.substring(0, 100),
        es_envase: esEnvase,
        fecha_sync: new Date().toISOString(),
      });

      // Classify insumo type
      let tipoGlobal = 'otro';
      if (detUp.includes('ENVASE') || detUp.includes('FRASCO')) tipoGlobal = 'frasco';
      else if (detUp.includes('CAJA') || detUp.includes('ESTUCHE') || detUp.includes('INTERIOR')) tipoGlobal = 'estuche';
      else if (detUp.includes('ESENCIA') || detUp.includes('FRAGANCIA') || detUp.includes('PERFUME')) tipoGlobal = 'esencia';
      else if (detUp.includes('TAPA')) tipoGlobal = 'tapa';
      else if (detUp.includes('COLLAR')) tipoGlobal = 'collar';
      else if (detUp.includes('VALVULA') || detUp.includes('PUMP') || detUp.includes('BOMBA')) tipoGlobal = 'valvula';

      // Lead time by type (days)
      let leadTimeDias = 90; // default
      if (tipoGlobal === 'frasco') leadTimeDias = 135; // 4.5 meses
      else if (tipoGlobal === 'estuche') leadTimeDias = 120; // 4 meses (China default)
      else if (tipoGlobal === 'esencia') leadTimeDias = 45; // 30-60 días

      // Store insumo stock — if same codigo appears multiple times, keep the one with higher stock
      if (!insumoStock[codigo] || stockFisico > (insumoStock[codigo].stock_fisico || 0)) {
        insumoStock[codigo] = {
          codigo: codigo,
          detalle: detalle.substring(0, 200),
          categoria: categoria,
          stock_fisico: stockFisico,
          stock_disponible: disponible,
          tipo_insumo_global: tipoGlobal,
          lead_time_dias: leadTimeDias,
          fecha_sync: new Date().toISOString(),
        };
      }
    }
  }

  // Upsert BOM in chunks
  let bomCount = 0;
  for (let i = 0; i < bomItems.length; i += 500) {
    const chunk = bomItems.slice(i, i + 500);
    const { error } = await supabase.from('bom_productos').upsert(chunk, { onConflict: 'codigo_principal,codigo_insumo' });
    if (!error) bomCount += chunk.length;
    else console.error('BOM upsert error:', error.message);
  }

  // Upsert stock_insumos
  const insumoArr = Object.values(insumoStock);
  const withStock = insumoArr.filter(i => i.stock_fisico > 0).length;
  const byTipo = {};
  insumoArr.forEach(i => { byTipo[i.tipo_insumo_global] = (byTipo[i.tipo_insumo_global] || 0) + 1; });
  console.log(`BOM insumos: ${insumoArr.length} unique, ${withStock} with stock > 0, types: ${JSON.stringify(byTipo)}`);
  // Log sample insumos for debugging
  const sample = insumoArr.filter(i => i.stock_fisico > 0).slice(0, 5);
  if (sample.length > 0) console.log('Sample insumos with stock:', JSON.stringify(sample.map(s => ({ cod: s.codigo, det: s.detalle?.substring(0,30), stk: s.stock_fisico, tipo: s.tipo_insumo_global }))));

  let insumoCount = 0;
  for (let i = 0; i < insumoArr.length; i += 500) {
    const chunk = insumoArr.slice(i, i + 500);
    const { error } = await supabase.from('stock_insumos').upsert(chunk, { onConflict: 'codigo' });
    if (!error) insumoCount += chunk.length;
    else console.error('Insumos upsert error:', error.message);
  }

  return { bom_rows: bomCount, insumos: insumoCount, sheet_used: usedSheet, with_stock: withStock, tipo_breakdown: byTipo };
}

// ─── SYNC 5: Producciones planificadas (UNIFICADO) ───
async function syncProducciones(sheets, config, supabase) {
  if (!config.sheet_id_producciones) return { updated: 0, skipped: 'No sheet_id_producciones configured' };

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: config.sheet_id_producciones,
    range: `'${config.sheet_name_producciones || 'UNIFICADO'}'`,
    valueRenderOption: 'UNFORMATTED_VALUE',
  });

  const rows = res.data.values || [];
  if (rows.length < 2) return { updated: 0, skipped: 'Not enough rows in Producciones' };

  // Row 0 = headers: CODIGO, PROVEEDOR, MARCA, DESCRIPCION, (blank), ABRIL, Junio 26, Julio 26, ...
  const headers = rows[0] || [];

  // Parse month columns (col 4+)
  const monthCols = [];
  const monthNames = {
    'ENERO': 0, 'FEBRERO': 1, 'MARZO': 2, 'ABRIL': 3, 'MAYO': 4, 'JUNIO': 5,
    'JULIO': 6, 'AGOSTO': 7, 'SEPTIEMBRE': 8, 'OCTUBRE': 9, 'NOVIEMBRE': 10, 'DICIEMBRE': 11
  };

  for (let c = 4; c < headers.length; c++) {
    const h = String(headers[c] || '').trim().toUpperCase();
    if (!h || h === 'COSTO') continue;

    // Try to parse month + year from header like "Junio 26" or "ABRIL"
    for (const [mName, mIdx] of Object.entries(monthNames)) {
      if (h.includes(mName)) {
        // Extract year: look for 2-digit or 4-digit year
        let year = new Date().getFullYear();
        const yearMatch = h.match(/(\d{2,4})/);
        if (yearMatch) {
          const y = parseInt(yearMatch[1]);
          year = y < 100 ? 2000 + y : y;
        }
        monthCols.push({ col: c, date: new Date(year, mIdx, 1) });
        break;
      }
    }
  }

  const producciones = [];
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r];
    const codigo = String(row[0] || '').trim();
    if (!codigo || isNaN(Number(codigo))) continue;

    const proveedor = String(row[1] || '').trim();
    const marca = String(row[2] || '').trim();
    const descripcion = String(row[3] || '').trim().substring(0, 200);

    for (const mc of monthCols) {
      const qty = parseNum(row[mc.col]);
      if (qty <= 0) continue;

      producciones.push({
        codigo: codigo,
        marca: marca,
        descripcion: descripcion,
        proveedor: proveedor,
        mes: mc.date.toISOString().substring(0, 10),
        cantidad: qty,
        fecha_sync: new Date().toISOString(),
      });
    }
  }

  if (producciones.length === 0) return { updated: 0, skipped: 'No production data found' };

  // Clear old and upsert
  let count = 0;
  for (let i = 0; i < producciones.length; i += 500) {
    const chunk = producciones.slice(i, i + 500);
    const { error } = await supabase.from('producciones_planificadas').upsert(chunk, { onConflict: 'codigo,mes' });
    if (!error) count += chunk.length;
    else console.error('Producciones upsert error:', error.message);
  }

  return { updated: count, months_parsed: monthCols.length };
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

    // Step 3: Sync BOM + Producciones in parallel
    const [bomResult, prodResult] = await Promise.allSettled([
      syncBOM(sheets, config, supabase),
      syncProducciones(sheets, config, supabase),
    ]);

    const bomRes = bomResult.status === 'fulfilled' ? bomResult.value : { error: bomResult.reason?.message };
    const prodRes = prodResult.status === 'fulfilled' ? prodResult.value : { error: prodResult.reason?.message };

    const allErrors = [stockSistemaRes.error, stockRes.error, forecastRes.error, bomRes.error, prodRes.error].filter(Boolean);
    const totalUpdated = (stockSistemaRes.updated || 0) + (stockRes.updated || 0) + (forecastRes.updated || 0);

    await supabase.from('stock_sync_log').insert({
      productos_actualizados: totalUpdated,
      errores: allErrors.join('; ') || null,
    });

    await supabase.from('config_sheets').update({ last_sync: new Date().toISOString() }).eq('id', 1);

    return res.status(200).json({
      message: 'Sync completed',
      stock_sistema: stockSistemaRes,
      stocks: stockRes,
      forecast: forecastRes,
      bom: bomRes,
      producciones: prodRes,
    });
  } catch (err) {
    console.error('Sync sheets error:', err);
    return res.status(500).json({ error: err.message });
  }
}
