import type { Request, Response } from 'express';
import { service } from '../services/questions.js';
import { ensureError } from '../utils/error.js';

const getAll = async (_req: Request, res: Response): Promise<void> => {
  try {
    const questions = await service.getAll();
    res.json(questions);
  } catch (err) {
    res.status(500).send(ensureError(err));
  }
};
const get = async (req: Request, res: Response): Promise<void> => {
  try {
    const { id } = req.params;
    const question = await service.get(id);
    res.json(question);
  } catch (err) {
    res.status(500).send(ensureError(err));
  }
};

export const controller = {
  getAll,
  get
};