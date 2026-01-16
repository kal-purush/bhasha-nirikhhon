// This file is used by Vercel to run the server in production
import express from 'express';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';
import { storage } from '../server/storage.js';

// Create Express app
const app = express();
const __dirname = dirname(fileURLToPath(import.meta.url));

// Middleware
app.use(express.json());

// API routes
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok' });
});

// Inquiry endpoints
app.post('/api/inquiries', async (req, res) => {
  try {
    const inquiry = await storage.createInquiry(req.body);
    res.status(201).json(inquiry);
  } catch (error) {
    res.status(400).json({ message: error.message });
  }
});

app.get('/api/inquiries', async (req, res) => {
  try {
    const inquiries = await storage.getInquiries();
    res.json(inquiries);
  } catch (error) {
    res.status(500).json({ message: error.message });
  }
});

// Static files
const staticDir = join(__dirname, '../dist/public');
app.use(express.static(staticDir));

// SPA fallback
app.get('*', (req, res) => {
  res.sendFile(join(staticDir, 'index.html'));
});

// Error handling
app.use((err, req, res, next) => {
  console.error(err);
  res.status(500).json({ message: 'Something went wrong', error: err.message });
});

// Export for Vercel serverless
export default app;