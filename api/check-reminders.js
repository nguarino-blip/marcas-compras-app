// GET /api/check-reminders (called by Vercel Cron daily at 12:00 UTC / 9:00 AR)
import { createClient } from '@supabase/supabase-js';
import { Resend } from 'resend';

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);
const resend = new Resend(process.env.RESEND_API_KEY);
const EMAIL_FROM = process.env.EMAIL_FROM || 'CDimex Compras <noreply@cdimex.com.ar>';

export default async function handler(req, res) {
  // Verify cron secret (Vercel sends this header for cron jobs)
  if (req.headers.authorization !== `Bearer ${process.env.CRON_SECRET}`) {
    // Also allow internal API key
    if (req.headers['x-api-key'] !== process.env.INTERNAL_API_KEY) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
  }

  try {
    // Get upcoming deadlines from DB function
    const { data: deadlines, error } = await supabase.rpc('get_upcoming_deadlines');
    if (error) throw error;

    if (!deadlines || deadlines.length === 0) {
      return res.status(200).json({ message: 'No deadlines to notify', sent: 0 });
    }

    let sent = 0;
    for (const d of deadlines) {
      // Determine recipients: responsible parties + creator
      const recipients = new Set();
      recipients.add(d.created_by_email);

      // Get all compras emails for compras-owned steps
      if (d.responsable === 'compras' || d.responsable === 'comex') {
        const { data: comprasUsers } = await supabase
          .from('profiles')
          .select('email')
          .eq('role', 'compras');
        comprasUsers?.forEach(u => recipients.add(u.email));
      }

      let urgency = '';
      let emoji = '';
      if (d.dias_restantes <= 0) {
        urgency = 'VENCIDO';
        emoji = '🔴';
      } else if (d.dias_restantes <= 10) {
        urgency = `Vence en ${d.dias_restantes} días`;
        emoji = '🟡';
      } else {
        urgency = `Vence en ${d.dias_restantes} días`;
        emoji = '🟠';
      }

      const html = `
        <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;">
          <div style="background:#1a1a2e;color:#f0c040;padding:20px;border-radius:8px 8px 0 0;">
            <h2 style="margin:0;">${emoji} Recordatorio de fecha - Solicitud #${d.solicitud_numero}</h2>
          </div>
          <div style="padding:20px;background:#f9f9f9;border-radius:0 0 8px 8px;">
            <p><strong>Solicitud:</strong> ${d.solicitud_nombre}</p>
            <p><strong>Paso:</strong> ${d.paso_nombre}</p>
            <p><strong>Fecha objetivo:</strong> ${d.fecha_objetivo}</p>
            <p style="font-size:18px;color:${d.dias_restantes <= 0 ? '#dc3545' : '#f0c040'};font-weight:bold;">
              ${urgency}
            </p>
            <p>Por favor revisa el estado de esta solicitud en la aplicación.</p>
          </div>
        </div>`;

      try {
        await resend.emails.send({
          from: EMAIL_FROM,
          to: Array.from(recipients),
          subject: `${emoji} ${urgency}: ${d.paso_nombre} - Solicitud #${d.solicitud_numero}`,
          html
        });
        sent++;
      } catch (emailErr) {
        console.error(`Failed to send reminder for ${d.solicitud_id}:`, emailErr);
      }
    }

    return res.status(200).json({ message: `Sent ${sent} reminders`, sent });
  } catch (err) {
    console.error('Reminder cron error:', err);
    return res.status(500).json({ error: err.message });
  }
}
