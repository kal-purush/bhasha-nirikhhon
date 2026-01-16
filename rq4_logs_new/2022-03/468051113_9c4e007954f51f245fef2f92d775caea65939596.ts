import AppError from '@shared/errors/AppError';
import { Request, Response } from 'express';
import { CreateBankService } from '../services/CreateBankService';
import { FindBankService } from '../services/FindBankService';

export class BankController {
  public async index(request: Request, response: Response): Promise<Response> {
    //const userId = request.user.id;
    const { userId } = request.query;
    if (!userId) throw new AppError('userId is required');
    const bankService = new FindBankService();
    const bank = await bankService.execute(+userId);
    return response.json(bank);
  }

  public async create(request: Request, response: Response): Promise<Response> {
    //const userId = request.user.id;
    const { userId } = request.query;
    const { name, nameBet, description } = request.body;
    if (!userId) throw new AppError('userId is required');
    const bankService = new CreateBankService();
    const bank = await bankService.execute(
      { name, nameBet, description },
      +userId,
    );
    return response.json(bank);
  }
}