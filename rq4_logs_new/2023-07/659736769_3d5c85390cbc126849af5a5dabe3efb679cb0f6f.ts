import { Income } from '@application/entities/income.entity';
import { IncomeRepository } from '@application/repositories/ income-repository';
import { CategoryRepository } from '@application/repositories/category-repository';
import { HttpException, Injectable } from '@nestjs/common';
import { parse } from 'date-fns';

interface CreateIncomeRequest {
  user_uuid: string;
  category_uuid: string;
  date: string;
  value: number;
  isReceived: boolean;
  description: string;
}

@Injectable()
export class CreateIncome {
  constructor(
    private incomeRepository: IncomeRepository,
    private categoryRepository: CategoryRepository,
  ) {}

  async execute({
    user_uuid,
    category_uuid,
    date,
    value,
    isReceived,
    description,
  }: CreateIncomeRequest): Promise<void> {
    const category = await this.categoryRepository.findCategoryById(
      category_uuid,
    );

    if (!category) {
      throw new HttpException('A categoria não foi encontrada!', 404);
    }

    const parsedDate = parse(date, 'dd/MM/yyyy', new Date());

    const income = new Income({
      date: parsedDate,
      value,
      isReceived,
      description,
      category,
    });

    await this.incomeRepository.create(user_uuid, income);
  }
}