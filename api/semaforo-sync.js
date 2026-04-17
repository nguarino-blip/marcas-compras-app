// POST /api/semaforo-sync
// Syncs semáforo (yellow/red alerts) to a dedicated Google Sheets tab
// Body: { rows: [[date, numero, nombre, marca, tipo, status, paso, responsable, dias, color], ...] }

import { google } from 'googleapis';

const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const SCOPES = ['https://www.googleapis.com/auth/spreadsheets'];

function getAuth() {
  const auth = new google.auth.JWT(
    process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
    null,
    process.env.GOOGLE_PRIVATE_KEY?.replace(/\\n/g, '\n'),
    SCOPES
  );
  return auth;
}

async function getSheets() {
  const auth = getAuth();
  return google.sheets({ version: 'v4', auth });
}

export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const authHeader = req.headers['x-api-key'];
  if (authHeader !== process.env.INTERNAL_API_KEY) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const { rows } = req.body;
  if (!rows?.length) return res.status(200).json({ success: true, message: 'No rows to sync' });

  try {
    const sheets = await getSheets();

    // Ensure "Semáforo" sheet exists
    const spreadsheet = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
    const semaforoSheet = spreadsheet.data.sheets.find(s => s.properties.title === 'Semáforo');

    if (!semaforoSheet) {
      // Create the sheet with headers
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: {
          requests: [{ addSheet: { properties: { title: 'Semáforo' } } }]
        }
      });
      // Add headers
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: 'Semáforo!A1:J1',
        valueInputOption: 'USER_ENTERED',
        requestBody: {
          values: [['Fecha', 'Nro', 'Nombre', 'Marca', 'Tipo', 'Estado', 'Paso pendiente', 'Responsable', 'Días restantes', 'Semáforo']]
        }
      });
    }

    // Append rows
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: 'Semáforo!A:J',
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: rows }
    });

    return res.status(200).json({ success: true, message: `${rows.length} rows synced` });
  } catch (err) {
    console.error('Semáforo sync error:', err);
    return res.status(500).json({ error: err.message });
  }
}
