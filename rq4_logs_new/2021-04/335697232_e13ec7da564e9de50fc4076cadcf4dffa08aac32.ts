/* eslint-disable @typescript-eslint/quotes */
import { RequestHandler } from 'express';

const CSPHeader = [
  `default-src 'self'`,
  `connect-src 'self' ws://localhost:54321 https://ya-praktikum.tech wss://ya-praktikum.tech`,
  `font-src 'self' https://fonts.gstatic.com https://fonts.googleapis.com`,
  `img-src 'self' blob: data: https://ya-praktikum.tech https://ssl.gstatic.com`,
  `style-src 'self' 'unsafe-inline' https://fonts.googleapis.com`,
  `script-src 'self' 'unsafe-inline'`,
];

export const cspHeader: RequestHandler = (_req, res, next) => {
  res.setHeader('Content-Security-Policy', CSPHeader.join(';'));
  next();
};