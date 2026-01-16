import type { Request, Response } from 'express';
import { service } from '../services/sleep-windows.js';
import { ensureError } from '../utils/error.js';

const get = async (req: Request, res: Response): Promise<void> => {
  try {
    const { id } = req.params;
    const window = await service.get(id);
    res.json(window);
  } catch (err) {
    res.status(500).send(ensureError(err));
  }
};

const getByEventId = async (req: Request, res: Response): Promise<void> => {
  try {
    const { eventId } = req.params;
    const windows = await service.getByEventId(eventId);
    res.json(windows);
  } catch (err) {
    res.status(500).send(ensureError(err));
  }
};

const create = async (req: Request, res: Response): Promise<void> => {
  try {
    const creationParams = req.body;

    const sleepWindowData = {
      event: {
        connect: { eventId: creationParams.eventId }
      },
      startTime: creationParams.startTime,
      stopTime: creationParams.stopTime,
      isSleep: creationParams.isSleep,
      note: creationParams.note ?? null
    };

    const window = await service.create(sleepWindowData);
    res.json(window);
  } catch (err) {
    res.status(500).send(ensureError(err));
  }
};

const update = async (req: Request, res: Response): Promise<void> => {
  try {
    const { id } = req.params;
    const updateParams = req.body;

    const window = await service.update(id, updateParams);
    res.json(window);
  } catch (err) {
    res.status(500).send(ensureError(err));
  }
};

const destroy = async (req: Request, res: Response): Promise<void> => {
  try {
    const { id } = req.params;
    const window = await service.destroy(id);
    res.json(window);
  } catch (err) {
    res.status(500).send(ensureError(err));
  }
};

export const controller = {
  get,
  getByEventId,
  create,
  update,
  destroy
};