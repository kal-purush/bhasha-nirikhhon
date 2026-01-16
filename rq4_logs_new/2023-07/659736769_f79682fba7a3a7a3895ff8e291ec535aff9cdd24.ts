import { Income } from '@application/entities/income.entity';
import { IncomeRepository } from '@application/repositories/ income-repository';
import { HttpException, Injectable } from '@nestjs/common';

interface DeleteIncomeByIdRequest {
  income_uuid: string;
}

@Injectable()
export class DeleteIncomeById {
  constructor(private incomeRepository: IncomeRepository) {}

  async execute({ income_uuid }: DeleteIncomeByIdRequest): Promise<void> {
    const income = await this.incomeRepository.findIncomeById(income_uuid);

    if (!income) {
      throw new HttpException(
        'Nenhuma receita foi encontrada com este uuid!',
        404,
      );
    }

    await this.incomeRepository.deleteIncomeById(income_uuid);
  }
}