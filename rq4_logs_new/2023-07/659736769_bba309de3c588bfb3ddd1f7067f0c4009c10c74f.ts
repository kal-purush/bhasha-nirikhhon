import { HttpException, Injectable } from '@nestjs/common';
import { IncomeRepository } from '@application/repositories/ income-repository';
import { CategoryRepository } from '@application/repositories/category-repository';
import { Income } from '@application/entities/income.entity';
import { parse } from 'date-fns';
import { Category } from '@application/entities/category.entity';

interface UpdateIncomeByIdRequest {
  income_uuid: string;
  category_uuid: string;
  description: string;
  date: string;
  value: number;
  isReceived: boolean;
}

@Injectable()
export class UpdateIncomeById {
  constructor(
    private incomeRepository: IncomeRepository,
    private categoryRepository: CategoryRepository,
  ) {}

  async execute({
    income_uuid,
    category_uuid,
    description,
    date,
    value,
    isReceived,
  }: UpdateIncomeByIdRequest) {
    let category: Category;

    const currentIncome = await this.incomeRepository.findIncomeById(
      income_uuid,
    );

    const checkIfCategoryIsSame = category_uuid != currentIncome.category.id;

    if (checkIfCategoryIsSame) {
      category = await this.categoryRepository.findCategoryById(category_uuid);

      if (!category) {
        throw new HttpException('A categoria não foi encontrada!', 404);
      }
    }

    const income = new Income(
      {
        createdAt: currentIncome.createdAt,
        value: value ?? currentIncome.value,
        isReceived: isReceived ?? currentIncome.isReceived,
        description: description ?? currentIncome.description,
        category: checkIfCategoryIsSame ? category : currentIncome.category,
        date: date ? parse(date, 'dd/MM/yyyy', new Date()) : currentIncome.date,
      },
      currentIncome.id,
    );

    await this.incomeRepository.update(income);
  }
}