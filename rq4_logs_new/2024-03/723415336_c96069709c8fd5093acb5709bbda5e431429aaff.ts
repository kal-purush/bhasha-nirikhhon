import { NextFunction, Request, Response } from 'express';
import quizService from '../services/quiz.service';
import { IdsSocials } from '@/types';
import { ClientError } from '@/errors';

const getPoliticalPhrasesByGroup = async (
  req: Request,
  res: Response,
  next: NextFunction,
): Promise<void> => {
  const { id } = req.body;
  try {
    if (!id) {
      throw new ClientError('Debe ingresar el id de una opcion de polinomio por body');
    }
    const phrases = await quizService.getPoliticalPhrases(id);
    res.status(200).json(phrases);
  } catch (error) {
    next(error);
  }
};

const getSocialPhrasesByGroup = async (
  req: Request,
  res: Response,
  next: NextFunction,
): Promise<void> => {
  const { ids }: IdsSocials = req.body;
  try {
    if (!ids) {
      throw new ClientError('Debe ingresar los id de las opciones de polinomios por body');
    }
    const phrases = await quizService.getSocialPhrases(ids);
    res.status(200).json({ phrases });
  } catch (error) {
    next(error);
  }
};

const getInverseSocialPhrasesByGroup = async (
  req: Request,
  res: Response,
  next: NextFunction,
): Promise<void> => {
  const { ids } = req.body;
  try {
    if (!ids) {
      throw new ClientError('Debe ingresar los id de las opciones de polinomios por body');
    }
    const phrases = await quizService.getInverseSocialPhrases(ids);
    res.status(200).json({ phrases });
  } catch (error) {
    next(error);
  }
};

const getInversePoliticalPhrasesByGroup = async (
  req: Request,
  res: Response,
  next: NextFunction,
): Promise<void> => {
  const { id } = req.body;
  try {
    if (!id) {
      throw new ClientError('Debe ingresar el id de una opcion de polinomio por body');
    }
    const phrases = await quizService.getInversePoliticalPhrases(id);
    res.status(200).json(phrases);
  } catch (error) {
    next(error);
  }
};

const getPoliticalNeutralPolarized = async (req: Request, res: Response, next: NextFunction) => {
  const { id } = req.body;
  try {
    if (!id) {
      throw new ClientError('Debe ingresar el id de una opcion de polinomio por body');
    }
    const combinatedPhrases = await quizService.getCombinedNeutralPoliticalPhrases(id);
    res.status(200).json(combinatedPhrases);
  } catch (error) {
    next(error);
  }
};

const getPoliticalNeutralInverse = async (req: Request, res: Response, next: NextFunction) => {
  const { id } = req.body;
  try {
    if (!id) {
      throw new ClientError('Debe ingresar el id de una opcion de polinomio por body');
    }
    const combinatedPhrases = await quizService.getCombinedNeutralPoliticalInverse(id);
    res.status(200).json(combinatedPhrases);
  } catch (error) {
    next(error);
  }
};

export default {
  getPoliticalNeutralPolarized,
  getPoliticalPhrasesByGroup,
  getInversePoliticalPhrasesByGroup,
  getSocialPhrasesByGroup,
  getInverseSocialPhrasesByGroup,
  getPoliticalNeutralInverse,
};