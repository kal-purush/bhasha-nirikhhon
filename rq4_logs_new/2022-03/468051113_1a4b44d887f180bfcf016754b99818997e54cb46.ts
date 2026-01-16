import AppError from '@shared/errors/AppError';
import { getCustomRepository } from 'typeorm';
import { BankRepository } from '../typeorm/repositories/BankRepository';

export class DeleteBankService {
  public async execute(bankId: number, userId: number): Promise<void>{
    const bankRepository = getCustomRepository(BankRepository);
    const bankExist = await bankRepository.findOne({ id: bankId });
    if (!bankExist) throw new AppError('Bank not found');
    await bankRepository.softDelete(bankId);
  }
}