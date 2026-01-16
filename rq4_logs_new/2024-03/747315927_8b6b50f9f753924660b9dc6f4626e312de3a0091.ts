import express from 'express';
import { controller } from '../controllers/sleep-windows.js';
import { auth } from '../services/auth.js';
import { asyncHandler } from './handler.js';

const router = express.Router();

// Protecting routes with auth0
router.use(auth.requireAuth);

// Routes
router.get('/:id', asyncHandler(controller.get));
router.get(
  '/sleep-windows/event/:eventId',
  asyncHandler(controller.getByEventId)
);
router.post('/create', asyncHandler(controller.create));
router.put('/:id/update', asyncHandler(controller.update));
router.delete('/:id/delete', asyncHandler(controller.destroy));

export default router;