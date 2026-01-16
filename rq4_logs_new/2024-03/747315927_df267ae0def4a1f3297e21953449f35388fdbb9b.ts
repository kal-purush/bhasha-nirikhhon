import type { Request, Response } from 'express';
import { service } from '../services/medicines.js';
import { ensureError } from '../utils/error.js';

const create = async (req: Request, res: Response): Promise<void> => {
  try {
    const creationParams = req.body;
    if (creationParams.length === 0) {
      throw new Error('Empty body');
    }

    const answerData = {
      medicineId: creationParams.answerId,
      name: creationParams.name,
      user: { connect: { userId: creationParams.userId } }
    };

    const answer = await service.create(answerData);
    res.json(answer);
  } catch (err) {
    res.status(500).send(ensureError(err));
  }
};

export const controller = {
  create
};