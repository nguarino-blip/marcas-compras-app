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

  // 1. Read description mapping from existing stock_productos in Supabase (already has names from forecast sync)
  let descMap = {};
  try {
    const { data: existingProds } = await supabase.from('stock_productos').select('codigo, nombre').neq('nombre', '');
    for (const p of (existingProds || [])) {
      if (p.codigo && p.nombre) descMap[p.codigo] = p.nombre;
    }
    console.log(`Stock Sistema: loaded ${Object.keys(descMap).length} product names from stock_productos`);
  } catch (e) {
    console.warn('Could not load product names:', e.message);
  }

  // 2. Read Stock pivot table: CODIGO | dep1 | dep2 | ... (sum all deposits)
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: config.sheet_id_stock_sistema,
    range: `'${config.sheet_name_stock_sistema || 'STOCK'}'`,
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

  // Return stockMap so handler can pass it to syncBOM for real stock values
  console.log(`Stock Sistema: ${updatedCount} products updated, stockMap has ${Object.keys(stockMap).length} codes, sample: ${JSON.stringify(Object.entries(stockMap).slice(0, 3).map(([k,v]) => [k, v.stock_actual]))}`);
  return { updated: updatedCount, matched_codes: matched.size, not_matched: notMatched, total_codes: updates.length, stock_map_size: Object.keys(stockMap).length, _stockMap: stockMap };
}

// ─── SYNC 2: Ventas/Stock from "Datos" flat table ───
// Sheet format: Polo | Segmento | Marca | Categoria | SKU | Descripción | Costo Neto | Precio de Venta |
//   Ingresos UND Mes | Reservas UND Mes | Stock por sistema mes | Stock General UND | Stock $ Venta |
//   Venta proyectada Mes UND | ... | Venta proyectada Mes+1 UND | ... | Venta proyectada Mes+2 UND | ...
async function syncStocks(sheets, config, supabase) {
  if (!config.sheet_id_stocks) return { updated: 0, skipped: 'No sheet_id_stocks configured' };

  const sheetNames = [
    config.sheet_name_stocks,
    'Datos',
    'Ventas stock cierre cobertura',
  ].filter(Boolean);

  let rows = [];
  let usedSheet = '';
  for (const sn of sheetNames) {
    try {
      const res = await sheets.spreadsheets.values.get({
        spreadsheetId: config.sheet_id_stocks,
        range: `'${sn}'`,
        valueRenderOption: 'UNFORMATTED_VALUE',
      });
      rows = res.data.values || [];
      if (rows.length >= 2) { usedSheet = sn; break; }
    } catch (e) {
      console.warn(`Stocks sheet '${sn}' not found, trying next...`);
    }
  }
  if (rows.length < 2) return { updated: 0, skipped: 'No stocks sheet found (tried: ' + sheetNames.join(', ') + ')' };
  console.log(`Stocks: using sheet '${usedSheet}' with ${rows.length} rows`);

  // Row 0 = headers — find columns by name
  const headers = (rows[0] || []).map(h => String(h || '').toUpperCase().trim());
  const col = (keywords) => {
    for (const kw of keywords) {
      const idx = headers.findIndex(h => h.includes(kw));
      if (idx >= 0) return idx;
    }
    return -1;
  };

  const iSKU = col(['SKU', 'CODIGO']);
  const iDesc = col(['DESCRIPCI', 'NOMBRE', 'DETALLE']);
  const iMarca = col(['MARCA']);
  const iSegmento = col(['SEGMENTO']);
  const iPolo = col(['POLO']);
  const iStockGeneral = col(['STOCK GENERAL UND', 'STOCK GENERAL', 'STOCK UND']);
  const iStockSistema = col(['STOCK POR SISTEMA', 'STOCK SISTEMA']);
  const iVtaMes = col(['VENTA PROYECTADA MES UND', 'VENTA PROYECTADA MES', 'VENTAS']);
  const iVtaMes1 = col(['VENTA PROYECTADA MES + 1 UND', 'MES + 1 UND', 'MES+1']);
  const iVtaMes2 = col(['VENTA PROYECTADA MES + 2 UND', 'MES + 2 UND', 'MES+2']);

  console.log('Stocks column indices:', JSON.stringify({ iSKU, iDesc, iMarca, iSegmento, iPolo, iStockGeneral, iStockSistema, iVtaMes, iVtaMes1, iVtaMes2 }));

  if (iSKU < 0 && iDesc < 0) {
    return { updated: 0, skipped: 'Could not find SKU or Description column in headers: ' + headers.slice(0, 15).join(', ') };
  }

  // Parse products
  const sheetProds = [];
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r];
    const sku = iSKU >= 0 ? String(row[iSKU] || '').trim() : '';
    const nombre = iDesc >= 0 ? String(row[iDesc] || '').trim() : '';
    const marca = iMarca >= 0 ? String(row[iMarca] || '').trim() : '';
    if (!nombre && !sku) continue;
    if (marca === 'TOTAL' || marca === 'Total' || nombre === 'TOTAL') continue;

    const stockGen = iStockGeneral >= 0 ? parseNum(row[iStockGeneral]) : 0;
    const stockSis = iStockSistema >= 0 ? parseNum(row[iStockSistema]) : 0;
    const vtaMes = iVtaMes >= 0 ? parseNum(row[iVtaMes]) : 0;
    const vtaMes1 = iVtaMes1 >= 0 ? parseNum(row[iVtaMes1]) : 0;
    const vtaMes2 = iVtaMes2 >= 0 ? parseNum(row[iVtaMes2]) : 0;

    // venta_mensual_avg = average of available monthly projections
    const vtaValues = [vtaMes, vtaMes1, vtaMes2].filter(v => v > 0);
    const ventaAvg = vtaValues.length > 0 ? Math.round(vtaValues.reduce((a, b) => a + b, 0) / vtaValues.length) : 0;

    // stock: prefer Stock General, fallback to Stock Sistema
    const stock = stockGen > 0 ? stockGen : stockSis;

    sheetProds.push({
      codigo: sku || nombre.substring(0, 30).replace(/\s+/g, '_').toUpperCase(),
      nombre,
      marca,
      segmento: iSegmento >= 0 ? String(row[iSegmento] || '').trim() : null,
      polo: iPolo >= 0 ? String(row[iPolo] || '').trim() : null,
      stock_actual: stock,
      venta_mensual_avg: ventaAvg,
      fecha_sync: new Date().toISOString(),
    });
  }

  if (sheetProds.length === 0) return { updated: 0, skipped: 'No products found in sheet' };
  console.log(`Stocks: parsed ${sheetProds.length} products from '${usedSheet}', ${sheetProds.filter(p => p.venta_mensual_avg > 0).length} with ventas > 0`);

  // Match by SKU code against existing stock_productos records
  const { data: existing } = await supabase.from('stock_productos').select('id, codigo, nombre, marca').range(0, 9999);
  const existByCodigo = {};
  const existByNombre = {};
  for (const ex of (existing || [])) {
    existByCodigo[ex.codigo] = ex;
    const nKey = (ex.nombre || '').toUpperCase().replace(/\s+/g, ' ').trim();
    if (nKey && !existByNombre[nKey]) existByNombre[nKey] = ex;
  }

  let updatedCount = 0;
  let insertedCount = 0;

  for (const sp of sheetProds) {
    // Match: 1) by codigo, 2) by nombre
    let match = existByCodigo[sp.codigo];
    if (!match) {
      const nKey = sp.nombre.toUpperCase().replace(/\s+/g, ' ').trim();
      match = existByNombre[nKey];
    }

    if (match) {
      const fields = { venta_mensual_avg: sp.venta_mensual_avg, fecha_sync: sp.fecha_sync };
      // Only update stock_actual from this sheet if Stock Sistema didn't already set it
      // (Stock Sistema has more reliable stock data from the ERP pivot)
      const { error } = await supabase.from('stock_productos').update(fields).eq('id', match.id);
      if (!error) updatedCount++;
    } else {
      // Insert new product with all fields
      const { error } = await supabase.from('stock_productos').upsert([sp], { onConflict: 'codigo,marca' });
      if (!error) insertedCount++;
    }
  }

  return { updated: updatedCount, inserted: insertedCount, total_sheet: sheetProds.length, sheet_used: usedSheet, with_ventas: sheetProds.filter(p => p.venta_mensual_avg > 0).length };
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
async function syncBOM(sheets, config, supabase, realStockMap) {
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

      // Classify insumo type based on DETALLE + CATEGORIA keywords
      const catUp = categoria.toUpperCase();
      let tipoGlobal = 'otro';
      if (detUp.includes('ENVASE') || detUp.includes('FRASCO') || detUp.includes('FLACON') || detUp.includes('BOTELLA')
          || catUp.includes('ENVASE') || catUp.includes('FRASCO') || catUp.includes('FLACON')) tipoGlobal = 'frasco';
      else if (detUp.includes('CAJA') || detUp.includes('ESTUCHE') || detUp.includes('INTERIOR') || detUp.includes('DISPLAY')
          || detUp.includes('PACKAGING') || detUp.includes('BLISTER') || detUp.includes('SOBRE')
          || catUp.includes('ESTUCHE') || catUp.includes('CAJA') || catUp.includes('PACKAGING')) tipoGlobal = 'estuche';
      else if (detUp.includes('ESENCIA') || detUp.includes('FRAGANCIA') || detUp.includes('PERFUME') || detUp.includes('PARFUM')
          || detUp.includes('EAU DE') || detUp.includes('CONCENTRADO') || detUp.includes('COMPOUND') || detUp.includes('ACEITE')
          || detUp.includes('BODY SPLASH') || detUp.includes('BODY MIST') || detUp.includes('COLONIA')
          || catUp.includes('ESENCIA') || catUp.includes('FRAGANCIA') || catUp.includes('CONCENTRADO')) tipoGlobal = 'esencia';
      else if (detUp.includes('TAPA') || detUp.includes('ROSCA') || catUp.includes('TAPA')) tipoGlobal = 'tapa';
      else if (detUp.includes('COLLAR') || detUp.includes('ANILLO') || detUp.includes('ARO') || catUp.includes('COLLAR')) tipoGlobal = 'collar';
      else if (detUp.includes('VALVULA') || detUp.includes('PUMP') || detUp.includes('BOMBA') || detUp.includes('SPRAY')
          || detUp.includes('DOSIFICADOR') || detUp.includes('ATOMIZADOR')
          || catUp.includes('VALVULA') || catUp.includes('PUMP')) tipoGlobal = 'valvula';
      else if (detUp.includes('ETIQUETA') || detUp.includes('STICKER') || detUp.includes('LABEL')
          || detUp.includes('SLEEVE') || detUp.includes('TERMOCONTRAIBLE')) tipoGlobal = 'etiqueta';
      else if (detUp.includes('BLOTTER') || detUp.includes('MUESTRA') || detUp.includes('PROBADOR')
          || detUp.includes('TESTER')) tipoGlobal = 'promo';

      // Lead time by type (days)
      let leadTimeDias = 90; // default
      if (tipoGlobal === 'frasco') leadTimeDias = 135; // 4.5 meses
      else if (tipoGlobal === 'estuche') leadTimeDias = 120; // 4 meses (China default)
      else if (tipoGlobal === 'esencia') leadTimeDias = 45; // 30-60 días
      else if (tipoGlobal === 'tapa' || tipoGlobal === 'collar' || tipoGlobal === 'valvula') leadTimeDias = 120;
      else if (tipoGlobal === 'etiqueta') leadTimeDias = 30;

      // Store insumo stock — use real stock from pivot table if available, fallback to BOM sheet value
      const realStk = realStockMap && realStockMap[codigo] ? realStockMap[codigo].stock_actual : null;
      const finalStockFisico = realStk != null ? realStk : stockFisico;
      const finalDisponible = realStk != null ? realStk : disponible;
      if (!insumoStock[codigo] || finalStockFisico > (insumoStock[codigo].stock_fisico || 0)) {
        insumoStock[codigo] = {
          codigo: codigo,
          detalle: detalle.substring(0, 200),
          categoria: categoria,
          stock_fisico: finalStockFisico,
          stock_disponible: finalDisponible,
          tipo_insumo_global: tipoGlobal,
          lead_time_dias: leadTimeDias,
          fecha_sync: new Date().toISOString(),
        };
      }
    }
  }

  // Deduplicate BOM by (codigo_principal, codigo_insumo) — keep last occurrence (most complete data)
  const bomDedup = {};
  bomItems.forEach(item => {
    const key = item.codigo_principal + '||' + item.codigo_insumo;
    bomDedup[key] = item; // last wins
  });
  const bomUnique = Object.values(bomDedup);
  console.log(`BOM: ${bomItems.length} raw rows → ${bomUnique.length} unique (principal,insumo) pairs from ${new Set(bomItems.map(b=>b.codigo_principal)).size} products`);

  // Clear old BOM data and re-insert (avoids stale rows from previous syncs)
  try {
    await supabase.from('bom_productos').delete().gt('id', '00000000-0000-0000-0000-000000000000');
    console.log('BOM: cleared old data');
  } catch (e) { console.warn('BOM clear failed (non-fatal):', e.message); }

  // Upsert BOM in small chunks (200) to avoid payload limits
  let bomCount = 0;
  const bomErrors = [];
  for (let i = 0; i < bomUnique.length; i += 200) {
    const chunk = bomUnique.slice(i, i + 200);
    const { error } = await supabase.from('bom_productos').upsert(chunk, { onConflict: 'codigo_principal,codigo_insumo' });
    if (!error) {
      bomCount += chunk.length;
    } else {
      console.error(`BOM upsert error (chunk ${Math.floor(i/200)+1}):`, error.message);
      bomErrors.push({ chunk: Math.floor(i/200)+1, from: i, to: i+chunk.length, error: error.message });
      // Try individual rows in failed chunk to find problematic ones
      let rescued = 0;
      for (const row of chunk) {
        const { error: e2 } = await supabase.from('bom_productos').upsert([row], { onConflict: 'codigo_principal,codigo_insumo' });
        if (!e2) rescued++;
      }
      bomCount += rescued;
      console.log(`  Rescued ${rescued}/${chunk.length} rows from failed chunk`);
    }
  }

  // Upsert stock_insumos
  const insumoArr = Object.values(insumoStock);
  const withStock = insumoArr.filter(i => i.stock_fisico > 0).length;
  const withRealStock = insumoArr.filter(i => realStockMap && realStockMap[i.codigo]).length;
  const byTipo = {};
  insumoArr.forEach(i => { byTipo[i.tipo_insumo_global] = (byTipo[i.tipo_insumo_global] || 0) + 1; });
  console.log(`BOM insumos: ${insumoArr.length} unique, ${withStock} with stock > 0, ${withRealStock} matched real stock map (map has ${realStockMap ? Object.keys(realStockMap).length : 0} codes), types: ${JSON.stringify(byTipo)}`);

  // Clear old insumos and re-insert
  try {
    await supabase.from('stock_insumos').delete().gt('id', '00000000-0000-0000-0000-000000000000');
  } catch (e) { console.warn('stock_insumos clear failed:', e.message); }

  let insumoCount = 0;
  for (let i = 0; i < insumoArr.length; i += 200) {
    const chunk = insumoArr.slice(i, i + 200);
    const { error } = await supabase.from('stock_insumos').upsert(chunk, { onConflict: 'codigo' });
    if (!error) insumoCount += chunk.length;
    else {
      console.error('Insumos upsert error:', error.message);
      // Retry individually
      for (const row of chunk) {
        const { error: e2 } = await supabase.from('stock_insumos').upsert([row], { onConflict: 'codigo' });
        if (!e2) insumoCount++;
      }
    }
  }

  return { bom_rows: bomCount, bom_raw: bomItems.length, bom_unique: bomUnique.length, bom_products: new Set(bomUnique.map(b=>b.codigo_principal)).size, insumos: insumoCount, sheet_used: usedSheet, with_stock: withStock, with_real_stock: withRealStock, real_stock_map_size: realStockMap ? Object.keys(realStockMap).length : 0, tipo_breakdown: byTipo, errors: bomErrors.length > 0 ? bomErrors : undefined };
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
    // Pass stockMap to syncBOM so it writes real stock values directly into stock_insumos
    // (avoids needing a separate update step that would cause timeout)
    const realStockMap = stockSistemaRes._stockMap || null;
    if (stockSistemaRes._stockMap) delete stockSistemaRes._stockMap;
    const [bomResult, prodResult] = await Promise.allSettled([
      syncBOM(sheets, config, supabase, realStockMap),
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
