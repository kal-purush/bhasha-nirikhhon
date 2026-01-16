import { Income } from '@application/entities/income.entity';
import { IncomeRepository } from '@application/repositories/ income-repository';
import { HttpException, Injectable } from '@nestjs/common';

interface FindIncomeByIdRequest {
  income_uuid: string;
}

interface FindIncomeByIdResponse {
  income: Income;
}

@Injectable()
export class FindIncomeById {
  constructor(private incomeRepository: IncomeRepository) {}

  async execute({
    income_uuid,
  }: FindIncomeByIdRequest): Promise<FindIncomeByIdResponse> {
    const income = await this.incomeRepository.findIncomeById(income_uuid);

    if (!income) {
      throw new HttpException('A receita não foi encontrada!', 404);
    }

    return { income };
  }
}