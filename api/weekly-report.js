// GET /api/weekly-report (called by Vercel Cron every Monday 9:00 UTC / 6:00 AR)
import { createClient } from '@supabase/supabase-js';
import { Resend } from 'resend';

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);
const resend = new Resend(process.env.RESEND_API_KEY);
const EMAIL_FROM = process.env.EMAIL_FROM || 'CDimex Compras <noreply@cdimex.com.ar>';

export default async function handler(req, res) {
  if (req.headers.authorization !== `Bearer ${process.env.CRON_SECRET}`) {
    if (req.headers['x-api-key'] !== process.env.INTERNAL_API_KEY) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
  }

  try {
    const { data: report, error } = await supabase.rpc('get_weekly_report_data');
    if (error) throw error;

    const vencidasHtml = report.vencidas?.length
      ? report.vencidas.map(v =>
          `<tr><td>#${v.numero}</td><td>${v.nombre}</td><td>${v.marca}</td><td>${v.paso}</td><td style="color:#dc3545;font-weight:bold;">${v.fecha_objetivo} (${v.dias_vencido}d)</td></tr>`
        ).join('')
      : '<tr><td colspan="5" style="text-align:center;color:#28a745;">Sin solicitudes vencidas</td></tr>';

    const proximasHtml = report.proximas_10d?.length
      ? report.proximas_10d.map(v =>
          `<tr><td>#${v.numero}</td><td>${v.nombre}</td><td>${v.marca}</td><td>${v.paso}</td><td style="color:#f0c040;font-weight:bold;">${v.fecha_objetivo} (${v.dias_restantes}d)</td></tr>`
        ).join('')
      : '<tr><td colspan="5" style="text-align:center;">Sin fechas próximas</td></tr>';

    const tableStyle = 'width:100%;border-collapse:collapse;margin:10px 0;';
    const thStyle = 'background:#1a1a2e;color:#f0c040;padding:8px;text-align:left;';
    const tdStyle = 'padding:8px;border-bottom:1px solid #ddd;';

    const html = `
      <div style="font-family:Arial,sans-serif;max-width:700px;margin:0 auto;">
        <div style="background:#1a1a2e;color:#f0c040;padding:20px;border-radius:8px 8px 0 0;">
          <h2 style="margin:0;">Reporte Semanal - Compras & Marcas</h2>
          <p style="margin:5px 0 0;color:#ccc;">Semana del ${new Date().toLocaleDateString('es-AR')}</p>
        </div>
        <div style="padding:20px;background:#f9f9f9;">
          <div style="display:flex;gap:20px;margin-bottom:20px;">
            <div style="flex:1;background:#fff;padding:15px;border-radius:8px;text-align:center;">
              <div style="font-size:28px;font-weight:bold;color:#1a1a2e;">${report.total_activas}</div>
              <div style="color:#666;">Solicitudes Activas</div>
            </div>
            <div style="flex:1;background:#fff;padding:15px;border-radius:8px;text-align:center;">
              <div style="font-size:28px;font-weight:bold;color:#dc3545;">${report.vencidas?.length || 0}</div>
              <div style="color:#666;">Fechas Vencidas</div>
            </div>
            <div style="flex:1;background:#fff;padding:15px;border-radius:8px;text-align:center;">
              <div style="font-size:28px;font-weight:bold;color:#28a745;">${report.completadas_semana}</div>
              <div style="color:#666;">Completadas esta semana</div>
            </div>
          </div>

          <h3 style="color:#dc3545;">Fechas Vencidas</h3>
          <table style="${tableStyle}">
            <tr><th style="${thStyle}">#</th><th style="${thStyle}">Solicitud</th><th style="${thStyle}">Marca</th><th style="${thStyle}">Paso</th><th style="${thStyle}">Vencimiento</th></tr>
            ${vencidasHtml}
          </table>

          <h3 style="color:#f0c040;">Próximas a vencer (10 días)</h3>
          <table style="${tableStyle}">
            <tr><th style="${thStyle}">#</th><th style="${thStyle}">Solicitud</th><th style="${thStyle}">Marca</th><th style="${thStyle}">Paso</th><th style="${thStyle}">Vencimiento</th></tr>
            ${proximasHtml}
          </table>
        </div>
        <div style="padding:10px 20px;background:#1a1a2e;border-radius:0 0 8px 8px;text-align:center;">
          <p style="color:#888;margin:0;font-size:12px;">CDimex - Sistema Marcas & Compras</p>
        </div>
      </div>`;

    // Send to all compras + admin users
    const { data: recipients } = await supabase
      .from('profiles')
      .select('email')
      .in('role', ['compras', 'admin']);

    const emails = recipients?.map(r => r.email) || ['nguarino@cdimex.com.ar'];

    await resend.emails.send({
      from: EMAIL_FROM,
      to: emails,
      subject: `Reporte Semanal Compras & Marcas - ${new Date().toLocaleDateString('es-AR')}`,
      html
    });

    return res.status(200).json({ message: 'Weekly report sent', recipients: emails.length });
  } catch (err) {
    console.error('Weekly report error:', err);
    return res.status(500).json({ error: err.message });
  }
}
