// pages/api/request-password-reset.ts

import { NextApiRequest, NextApiResponse } from 'next';
import nodemailer from 'nodemailer';
import { v4 as uuidv4 } from 'uuid';
import { Pool } from 'pg';

// ✅ חשוב: ודא שהחיבור כולל SSL
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false, // כדי ש-Neon יעבוד. אל תשכח!
  },
});

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const { email } = req.body;

  if (!email) {
    return res.status(400).json({ error: 'Email is required' });
  }

  try {
    console.log('Connecting to database with URL:', process.env.DATABASE_URL);
    const client = await pool.connect();

    const userResult = await client.query('SELECT * FROM users WHERE email = $1', [email]);

    if (userResult.rows.length === 0) {
      client.release();
      return res.status(200).json({ message: 'If user exists, a reset link was sent' });
    }

    const token = uuidv4();
    const expiry = new Date(Date.now() + 1000 * 60 * 60); // 1 hour from now

    await client.query(
      'UPDATE users SET reset_token = $1, reset_token_expiry = $2 WHERE email = $3',
      [token, expiry, email]
    );

    const transporter = nodemailer.createTransport({
      service: 'gmail',
      auth: {
        user: 'moshe.aaa4142@gmail.com',
        pass: process.env.GMAIL_APP_PASSWORD, // הגדר ב-.env
      },
    });

    const resetUrl = `http://localhost:3000/reset-password/${token}`;

    await transporter.sendMail({
      from: '"My Healthcare System" <moshe.aaa4142@gmail.com>',
      to: email,
      subject: 'Password Reset Request',
      html: `
        <h2>Password Reset</h2>
        <p>Click the link below to reset your password:</p>
        <a href="${resetUrl}">${resetUrl}</a>
        <p>This link will expire in 1 hour.</p>
      `,
    });

    client.release();
    return res.status(200).json({ message: 'If user exists, a reset link was sent' });
  } catch (error) {
    console.error('Reset error:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
}