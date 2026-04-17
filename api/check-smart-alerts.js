// GET /api/check-smart-alerts (daily cron alongside check-reminders)
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
    const { data: alerts, error } = await supabase.rpc('get_smart_alerts');
    if (error) throw error;
 
    let sent = 0;
 
    // 1. Solicitudes sin movimiento (5+ días)
    const sinMov = alerts?.sin_movimiento || [];
    if (sinMov.length > 0) {
      // Get compras team emails
      const { data: comprasUsers } = await supabase
        .from('profiles')
        .select('email')
        .in('role', ['compras', 'admin']);
      const recipients = [...new Set((comprasUsers || []).map(u => u.email))];
 
      if (recipients.length) {
        const rows = sinMov.map(a =>
          `<tr><td>#${a.numero}</td><td>${a.nombre}</td><td>${a.marca}</td><td>${a.status}</td><td style="color:#dc3545;font-weight:bold;">${a.dias_sin_movimiento}d</td></tr>`
        ).join('');
 
        await resend.emails.send({
          from: EMAIL_FROM,
          to: recipients,
          subject: `⚠️ ${sinMov.length} solicitudes sin movimiento (+5 días)`,
          html: `<div style="font-family:Arial;max-width:600px;margin:0 auto;">
            <div style="background:#1a1a2e;color:#f0c040;padding:20px;border-radius:8px 8px 0 0;">
              <h2 style="margin:0;">⚠️ Solicitudes estancadas</h2>
            </div>
            <div style="padding:20px;background:#f9f9f9;border-radius:0 0 8px 8px;">
              <p>${sinMov.length} solicitudes llevan más de 5 días sin actividad:</p>
              <table style="width:100%;border-collapse:collapse;font-size:13px;">
                <tr style="background:#eee;"><th>Nro</th><th>Nombre</th><th>Marca</th><th>Estado</th><th>Días</th></tr>
                ${rows}
              </table>
            </div>
          </div>`
        });
        sent++;
      }
    }
 
    // 2. Inspecciones desaprobadas con plazo vencido
    const inspVencidas = alerts?.inspeccion_vencida || [];
    if (inspVencidas.length > 0) {
      for (const insp of inspVencidas) {
        // Extract plazo from notas and check if expired
        const plazoMatch = insp.notas?.match(/\[PLAZO: (.+?)\]/);
        if (plazoMatch) {
          const plazo = plazoMatch[1];
          // Check if plazo has passed (simplified: just alert about existence)
          const recipients = [insp.created_by_email].filter(Boolean);
          if (recipients.length) {
            await resend.emails.send({
              from: EMAIL_FROM,
              to: recipients,
              subject: `🔴 Inspección desaprobada pendiente - Solicitud #${insp.numero}`,
              html: `<div style="font-family:Arial;max-width:600px;margin:0 auto;">
                <div style="background:#1a1a2e;color:#dc3545;padding:20px;border-radius:8px 8px 0 0;">
                  <h2 style="margin:0;">🔴 Inspección requiere re-aprobación</h2>
                </div>
                <div style="padding:20px;background:#f9f9f9;border-radius:0 0 8px 8px;">
                  <p><strong>Solicitud:</strong> #${insp.numero} - ${insp.nombre}</p>
                  <p><strong>Tipo inspección:</strong> ${insp.tipo}</p>
                  <p><strong>Plazo original:</strong> ${plazo}</p>
                  <p>Por favor revisá esta inspección en la aplicación.</p>
                </div>
              </div>`
            });
            sent++;
          }
        }
      }
    }
 
    return res.status(200).json({ message: `Smart alerts sent: ${sent}`, sinMov: sinMov.length, inspVencidas: inspVencidas.length });
  } catch (err) {
    console.error('Smart alerts error:', err);
    return res.status(500).json({ error: err.message });
  }
}
 
