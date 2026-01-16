import { NextFunction as NextFunc, Request, Response } from 'express';
import { successResponse } from '../helpers';
import { OK } from 'http-status';
import { BlockNumberResponse, Transaction } from '../interfaces';
import { BlockChainService } from '../services';

class BlockChainController {
  static async getBlockNumber(_: Request, res: Response, next: NextFunc) {
    try {
      const result = await new BlockChainService().getLatestBlockNumber();

      successResponse<BlockNumberResponse>(
        res,
        OK,
        'Block Number retrieved ✅',
        result,
      );
    } catch (error) {
      return next(error);
    }
  }

  static async getBlockTransactions(req: Request, res: Response, next: NextFunc) {
    const { blockNo } = req.params;

    try {
      const result = await new BlockChainService().getBlockTransactions(blockNo);

      successResponse<Transaction[]>(
        res,
        OK,
        'Block Transactions retrieved ✅',
        result,
      );
    } catch (error) {
      return next(error);
    }
  }
}

export { BlockChainController };