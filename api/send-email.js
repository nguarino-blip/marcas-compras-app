// POST /api/send-email
// Body: { to: string[], subject: string, html: string }
import { Resend } from 'resend';

const resend = new Resend(process.env.RESEND_API_KEY);

export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  // Verify request comes from our app (simple shared secret)
  const authHeader = req.headers['x-api-key'];
  if (authHeader !== process.env.INTERNAL_API_KEY) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const { to, subject, html } = req.body;
  if (!to || !subject || !html) {
    return res.status(400).json({ error: 'Missing to, subject or html' });
  }

  try {
    const data = await resend.emails.send({
      from: process.env.EMAIL_FROM || 'CDimex Compras <noreply@cdimex.com.ar>',
      to: Array.isArray(to) ? to : [to],
      subject,
      html
    });
    return res.status(200).json({ success: true, data });
  } catch (error) {
    console.error('Email error:', error);
    return res.status(500).json({ error: error.message });
  }
}
