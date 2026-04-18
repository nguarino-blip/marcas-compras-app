// GET /api/stock-alerts — Daily cron: email alerts for items at risk of stockout
import { createClient } from '@supabase/supabase-js';
import { Resend } from 'resend';

function getSupabase() {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL || process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
  if (!url || !key) throw new Error(`Missing Supabase env vars. URL=${!!url}, KEY=${!!key}`);
  return createClient(url, key);
}

const EMAIL_FROM = process.env.EMAIL_FROM || 'CDimex Compras <noreply@cdimex.com.ar>';

export default async function handler(req, res) {
  if (req.headers.authorization !== `Bearer ${process.env.CRON_SECRET}`) {
    if (req.headers['x-api-key'] !== process.env.INTERNAL_API_KEY) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
  }

  try {
    const supabase = getSupabase();
    const resend = new Resend(process.env.RESEND_API_KEY);
    const { data: alertData, error } = await supabase.rpc('get_stock_alerts_data');
    if (error) throw error;

    const alertas = alertData?.alertas_quiebre || [];
    if (alertas.length === 0) {
      return res.status(200).json({ message: 'No stock alerts to send', count: 0 });
    }

    // Separate by severity
    const criticos = alertas.filter(a => a.nivel_riesgo === 'SIN STOCK' || a.nivel_riesgo === 'CRITICO');
    const alerta = alertas.filter(a => a.nivel_riesgo === 'ALERTA');
    const atencion = alertas.filter(a => a.nivel_riesgo === 'ATENCIÓN');

    // Get compras team emails
    const { data: comprasUsers } = await supabase
      .from('profiles')
      .select('email')
      .in('role', ['compras', 'admin']);
    const recipients = [...new Set((comprasUsers || []).map(u => u.email))];

    if (recipients.length === 0) {
      return res.status(200).json({ message: 'No recipients found', count: alertas.length });
    }

    const riskColor = (nivel) => {
      switch (nivel) {
        case 'SIN STOCK': return '#dc3545';
        case 'CRITICO': return '#dc3545';
        case 'ALERTA': return '#f97316';
        case 'ATENCIÓN': return '#f0c040';
        default: return '#22c55e';
      }
    };

    const buildRows = (items) => items.map(a => `
      <tr style="border-bottom:1px solid #eee;">
        <td style="padding:6px 8px;">${a.codigo}</td>
        <td style="padding:6px 8px;">${a.nombre}</td>
        <td style="padding:6px 8px;">${a.marca}</td>
        <td style="padding:6px 8px;text-align:right;">${Math.round(a.stock_actual).toLocaleString()}</td>
        <td style="padding:6px 8px;text-align:right;">${a.cobertura_meses}m</td>
        <td style="padding:6px 8px;text-align:center;">
          <span style="background:${riskColor(a.nivel_riesgo)}22;color:${riskColor(a.nivel_riesgo)};padding:2px 8px;border-radius:12px;font-size:11px;font-weight:bold;">
            ${a.nivel_riesgo}
          </span>
        </td>
        <td style="padding:6px 8px;text-align:right;font-weight:bold;">${a.cantidad_sugerida_compra > 0 ? Math.round(a.cantidad_sugerida_compra).toLocaleString() : '-'}</td>
      </tr>
    `).join('');

    const subject = criticos.length > 0
      ? `🔴 ${criticos.length} productos sin stock / críticos + ${alerta.length} en alerta`
      : `⚠️ ${alerta.length} productos en alerta de quiebre`;

    const html = `
      <div style="font-family:Arial,sans-serif;max-width:800px;margin:0 auto;">
        <div style="background:#1a1a2e;color:#f0c040;padding:20px;border-radius:8px 8px 0 0;">
          <h2 style="margin:0;">📦 Alerta de Stock — Riesgo de Quiebre</h2>
          <p style="margin:8px 0 0;color:#ccc;font-size:13px;">
            ${alertas.length} productos requieren atención • Sync: ${new Date().toLocaleDateString('es-AR')}
          </p>
        </div>
        <div style="padding:20px;background:#f9f9f9;border-radius:0 0 8px 8px;">
          <div style="display:flex;gap:16px;margin-bottom:20px;">
            <div style="flex:1;background:#dc354522;border-radius:8px;padding:12px;text-align:center;">
              <div style="font-size:24px;font-weight:bold;color:#dc3545;">${criticos.length}</div>
              <div style="font-size:11px;color:#666;">Críticos / Sin Stock</div>
            </div>
            <div style="flex:1;background:#f9731622;border-radius:8px;padding:12px;text-align:center;">
              <div style="font-size:24px;font-weight:bold;color:#f97316;">${alerta.length}</div>
              <div style="font-size:11px;color:#666;">En Alerta</div>
            </div>
            <div style="flex:1;background:#f0c04022;border-radius:8px;padding:12px;text-align:center;">
              <div style="font-size:24px;font-weight:bold;color:#f0c040;">${atencion.length}</div>
              <div style="font-size:11px;color:#666;">Atención</div>
            </div>
          </div>

          ${criticos.length > 0 ? `
          <h3 style="color:#dc3545;margin-bottom:8px;">🔴 Críticos / Sin Stock</h3>
          <table style="width:100%;border-collapse:collapse;font-size:12px;margin-bottom:20px;">
            <tr style="background:#eee;"><th style="padding:6px 8px;text-align:left;">Código</th><th style="padding:6px 8px;text-align:left;">Producto</th><th style="padding:6px 8px;text-align:left;">Marca</th><th style="padding:6px 8px;text-align:right;">Stock</th><th style="padding:6px 8px;text-align:right;">Cobertura</th><th style="padding:6px 8px;text-align:center;">Nivel</th><th style="padding:6px 8px;text-align:right;">Compra sugerida</th></tr>
            ${buildRows(criticos)}
          </table>` : ''}

          ${alerta.length > 0 ? `
          <h3 style="color:#f97316;margin-bottom:8px;">⚠️ En Alerta</h3>
          <table style="width:100%;border-collapse:collapse;font-size:12px;margin-bottom:20px;">
            <tr style="background:#eee;"><th style="padding:6px 8px;text-align:left;">Código</th><th style="padding:6px 8px;text-align:left;">Producto</th><th style="padding:6px 8px;text-align:left;">Marca</th><th style="padding:6px 8px;text-align:right;">Stock</th><th style="padding:6px 8px;text-align:right;">Cobertura</th><th style="padding:6px 8px;text-align:center;">Nivel</th><th style="padding:6px 8px;text-align:right;">Compra sugerida</th></tr>
            ${buildRows(alerta)}
          </table>` : ''}

          ${atencion.length > 0 ? `
          <h3 style="color:#f0c040;margin-bottom:8px;">🟡 Atención (cobertura &lt; lead time)</h3>
          <table style="width:100%;border-collapse:collapse;font-size:12px;">
            <tr style="background:#eee;"><th style="padding:6px 8px;text-align:left;">Código</th><th style="padding:6px 8px;text-align:left;">Producto</th><th style="padding:6px 8px;text-align:left;">Marca</th><th style="padding:6px 8px;text-align:right;">Stock</th><th style="padding:6px 8px;text-align:right;">Cobertura</th><th style="padding:6px 8px;text-align:center;">Nivel</th><th style="padding:6px 8px;text-align:right;">Compra sugerida</th></tr>
            ${buildRows(atencion)}
          </table>` : ''}

          <p style="margin-top:20px;font-size:12px;color:#666;">
            💡 Compra sugerida = (Lead time × Demanda mensual) − Stock actual
          </p>
        </div>
      </div>
    `;

    await resend.emails.send({
      from: EMAIL_FROM,
      to: recipients,
      subject,
      html,
    });

    return res.status(200).json({
      message: `Stock alert sent to ${recipients.length} recipients`,
      count: alertas.length,
      criticos: criticos.length,
      alerta: alerta.length,
      atencion: atencion.length,
    });
  } catch (err) {
    console.error('Stock alerts error:', err);
    return res.status(500).json({ error: err.message });
  }
}
