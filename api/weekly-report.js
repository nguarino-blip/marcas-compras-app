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

    const tableStyle = 'width:100%;border-collapse:collapse;margin:10px 0;';
    const thStyle = 'background:#1a1a2e;color:#f0c040;padding:8px;text-align:left;font-size:13px;';
    const tdStyle = 'padding:8px;border-bottom:1px solid #ddd;font-size:13px;';

    // ROJO: Vencidas
    const rojasHtml = report.vencidas?.length
      ? report.vencidas.map(v =>
          `<tr>
            <td style="${tdStyle}">#${v.numero}</td>
            <td style="${tdStyle}">${v.nombre}</td>
            <td style="${tdStyle}">${v.marca}</td>
            <td style="${tdStyle}"><strong>${v.paso}</strong></td>
            <td style="${tdStyle};color:#dc3545;font-weight:bold;">Vencido hace ${v.dias_vencido}d<br><small>${v.fecha_objetivo}</small></td>
          </tr>`
        ).join('')
      : '<tr><td colspan="5" style="text-align:center;color:#28a745;padding:12px;">Sin solicitudes vencidas</td></tr>';

    // AMARILLO: Próximas 10 días
    const amarillasHtml = report.proximas_10d?.length
      ? report.proximas_10d.map(v =>
          `<tr>
            <td style="${tdStyle}">#${v.numero}</td>
            <td style="${tdStyle}">${v.nombre}</td>
            <td style="${tdStyle}">${v.marca}</td>
            <td style="${tdStyle}"><strong>${v.paso}</strong></td>
            <td style="${tdStyle};color:#e67e22;font-weight:bold;">${v.dias_restantes}d restantes<br><small>${v.fecha_objetivo}</small></td>
          </tr>`
        ).join('')
      : '<tr><td colspan="5" style="text-align:center;padding:12px;">Sin fechas próximas</td></tr>';

    const html = `
      <div style="font-family:Arial,sans-serif;max-width:750px;margin:0 auto;">
        <div style="background:#1a1a2e;color:#f0c040;padding:20px;border-radius:8px 8px 0 0;">
          <h2 style="margin:0;">Reporte Semanal — Semáforo de tiempos</h2>
          <p style="margin:5px 0 0;color:#ccc;">Semana del ${new Date().toLocaleDateString('es-AR')}</p>
        </div>
        <div style="padding:20px;background:#f9f9f9;">

          <table style="width:100%;border-collapse:collapse;margin-bottom:20px;">
            <tr>
              <td style="background:#fff;padding:18px;text-align:center;border-radius:8px;width:33%;">
                <div style="font-size:32px;font-weight:bold;color:#1a1a2e;">${report.total_activas}</div>
                <div style="color:#666;font-size:13px;">Activas</div>
              </td>
              <td style="width:8px;"></td>
              <td style="background:#fff4f4;padding:18px;text-align:center;border-radius:8px;border:2px solid #dc3545;width:33%;">
                <div style="font-size:32px;font-weight:bold;color:#dc3545;">${report.vencidas?.length || 0}</div>
                <div style="color:#666;font-size:13px;">Vencidas (ROJO)</div>
              </td>
              <td style="width:8px;"></td>
              <td style="background:#fff8f0;padding:18px;text-align:center;border-radius:8px;border:2px solid #e67e22;width:33%;">
                <div style="font-size:32px;font-weight:bold;color:#e67e22;">${report.proximas_10d?.length || 0}</div>
                <div style="color:#666;font-size:13px;">Próximas a vencer (AMARILLO)</div>
              </td>
            </tr>
          </table>

          <div style="margin-bottom:20px;">
            <h3 style="color:#dc3545;margin-bottom:8px;display:flex;align-items:center;gap:8px;">
              <span style="display:inline-block;width:12px;height:12px;border-radius:50%;background:#dc3545;"></span>
              ROJO — Entregables vencidos
            </h3>
            <table style="${tableStyle}">
              <tr><th style="${thStyle}">#</th><th style="${thStyle}">Solicitud</th><th style="${thStyle}">Marca</th><th style="${thStyle}">Entregable pendiente</th><th style="${thStyle}">Vencimiento</th></tr>
              ${rojasHtml}
            </table>
          </div>

          <div style="margin-bottom:20px;">
            <h3 style="color:#e67e22;margin-bottom:8px;display:flex;align-items:center;gap:8px;">
              <span style="display:inline-block;width:12px;height:12px;border-radius:50%;background:#e67e22;"></span>
              AMARILLO — Próximos a vencer (10 días)
            </h3>
            <table style="${tableStyle}">
              <tr><th style="${thStyle}">#</th><th style="${thStyle}">Solicitud</th><th style="${thStyle}">Marca</th><th style="${thStyle}">Entregable pendiente</th><th style="${thStyle}">Vencimiento</th></tr>
              ${amarillasHtml}
            </table>
          </div>

          <div style="background:#f0fff4;border:1px solid #28a745;border-radius:8px;padding:12px;text-align:center;">
            <span style="color:#28a745;font-weight:bold;">Completadas esta semana: ${report.completadas_semana}</span>
          </div>

        </div>
        <div style="padding:10px 20px;background:#1a1a2e;border-radius:0 0 8px 8px;text-align:center;">
          <p style="color:#888;margin:0;font-size:12px;">CDimex — Sistema Marcas & Compras · Reporte automático</p>
        </div>
      </div>`;

    // Send to compras team + marcas managers
    const { data: recipients } = await supabase
      .from('profiles')
      .select('email')
      .in('role', ['compras', 'admin']);

    const emails = recipients?.map(r => r.email) || ['nguarino@cdimex.com.ar'];

    await resend.emails.send({
      from: EMAIL_FROM,
      to: emails,
      subject: `[Semáforo] Reporte Semanal Compras & Marcas - ${new Date().toLocaleDateString('es-AR')}`,
      html
    });

    return res.status(200).json({ message: 'Weekly report sent', recipients: emails.length });
  } catch (err) {
    console.error('Weekly report error:', err);
    return res.status(500).json({ error: err.message });
  }
}
