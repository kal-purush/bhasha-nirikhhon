import { HttpException, Injectable } from '@nestjs/common';
import { Income } from '@application/entities/income.entity';
import { startOfMonth, endOfMonth, parse } from 'date-fns';
import { IncomeRepository } from '@application/repositories/ income-repository';

interface GetIncomesOfMonthRequest {
  user_uuid: string;
  month: string;
  year: string;
}

interface GetIncomesOfMonthResponse {
  incomesOfMonth: Income[];
}

@Injectable()
export class GetIncomesOfMonth {
  constructor(private incomeRepository: IncomeRepository) {}

  async execute({
    user_uuid,
    month,
    year,
  }: GetIncomesOfMonthRequest): Promise<GetIncomesOfMonthResponse> {
    const initialDate = startOfMonth(
      parse(`${year}-${month}`, 'yyyy-MM', new Date()),
    );

    const finalDate = endOfMonth(
      parse(`${year}-${month}`, 'yyyy-MM', new Date()),
    );

    const incomesOfMonth = await this.incomeRepository.getIncomesOfMonth(
      user_uuid,
      initialDate,
      finalDate,
    );

    if (incomesOfMonth.length === 0) {
      throw new HttpException(
        'Nenhuma receita foi encontrada no mês informado!',
        404,
      );
    }

    return { incomesOfMonth };
  }
}