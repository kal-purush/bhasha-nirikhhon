import { Request, Response, NextFunction } from 'express';
import resFormat from '../utils/resFormat';
import * as OrderRepository from '../repositories/OrderRepository';

export const existOrder = async (
  req: Request,
  res: Response,
  next: NextFunction
) => {
  const check = await OrderRepository.findByUserAndBookItem(
    req.user!.id,
    parseInt(req.params.id, 10)
  );
  if (!check)
    return res.status(401).send(resFormat.fail(401, '구매내역이 없습니다.'));
  next();
  return;
};