import { Request } from 'express';
import { inject } from 'inversify';
import { controller, httpPatch } from 'inversify-express-utils';
import { ITransactionService } from '../core/interfaces';
import { TYPES } from '../core/types';

@controller('/transactions')
export class TransactionController {
  @inject(TYPES.ITransactionService)
  private transactionService: ITransactionService;

  @httpPatch('/users/:userId/purchase')
  async purchase(req: Request): Promise<string> {
    const { userId } = req.params;
    const { quantity } = req.body;

    return await this.transactionService.purchase(userId, quantity);
  }
}