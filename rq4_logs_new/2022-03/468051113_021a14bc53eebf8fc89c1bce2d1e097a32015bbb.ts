import AppError from '@shared/errors/AppError';
import { getCustomRepository } from 'typeorm';
import { BankDto } from '../dto/BankDto';
import { BankRepository } from '../typeorm/repositories/BankRepository';

export class UpdateBankService {
  public async execute(bankId: number, userId: number, updateDto: BankDto) {
    const bankRepository = getCustomRepository(BankRepository);
    try {
      const bankExist = await bankRepository.findOneByUser(bankId, userId);
      console.log({ 'bank exist': bankExist });
      if (!bankExist) throw new AppError('Bank not Found');
      const bank = await bankRepository.save({ ...updateDto, id: bankId });
      return bank;
    } catch (error) {
      console.log(error);
    }
  }
}