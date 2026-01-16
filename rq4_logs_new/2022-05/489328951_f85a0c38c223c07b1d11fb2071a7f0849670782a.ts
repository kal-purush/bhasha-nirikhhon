import type { NextApiRequest, NextApiResponse } from 'next';
import { google } from 'googleapis';
import { Asset } from '../../types/asset';

const email = process.env.GOOGLE_CLIENT_EMAIL;
const key = process.env.GOOGLE_PRIVATE_KEY?.replace(/\\n/g, '\n');

export default async function handler(req: NextApiRequest, res: NextApiResponse<Asset[] | undefined>) {
  const auth = new google.auth.GoogleAuth({
    credentials: {
      client_email: email,
      private_key: key?.replace(/\\n/g, '\n'),
    },
    scopes: [
      'https://www.googleapis.com/auth/drive',
      'https://www.googleapis.com/auth/drive.file',
      'https://www.googleapis.com/auth/spreadsheets',
    ],
  });
  const client = await auth.getClient();
  const sheets = google.sheets({ version: 'v4', auth: client });
  const spreadsheetId = process.env.GOOGLE_SHEET_ID;

  const getRows = await sheets.spreadsheets.values.get({
    auth,
    spreadsheetId,
    range: '②ポートフォリオ',
  });

  const rows = getRows.data.values;
  const exe = [0];
  for (let i = 0; i < exe.length; i++) {
    rows?.splice(exe[i] - i, 1);
  }

  const assetList = rows?.map((row) => {
    return {
      code: Number(row[0]),
      name: String(row[1]),
      industry: String(row[2]),
      numberOfPossessions: Number(row[3]),
      purchasePrice: Number(row[4]),
      marketPrice: Number(row[5]),
      dividend: Number(row[6]),
    };
  });

  res.status(200).json(assetList);
}