import { Income } from '@application/entities/income.entity';
import { IncomeRepository } from '@application/repositories/ income-repository';
import { PrismaService } from '@infra/database/prisma.service';
import { Injectable } from '@nestjs/common';
import { PrismaIncomeMapper } from '../mappers/prisma-income-mapper';

@Injectable()
export class PrismaIncomeRepository implements IncomeRepository {
  constructor(private prisma: PrismaService) {}

  async create(user_uuid: string, income: Income): Promise<void> {
    const rawIncome = PrismaIncomeMapper.toPrisma(income);

    await this.prisma.income.create({
      data: { ...rawIncome, userId: user_uuid },
    });
  }

  async update(income: Income): Promise<void> {
    const rawIncome = PrismaIncomeMapper.toPrisma(income);

    await this.prisma.income.update({
      where: {
        id: rawIncome.id,
      },
      data: rawIncome,
    });
  }

  async findIncomeById(income_uuid: string): Promise<Income | null> {
    const income = await this.prisma.income.findUnique({
      where: {
        id: income_uuid,
      },
    });

    if (!income) {
      return null;
    }

    const category = await this.prisma.category.findUnique({
      where: {
        id: income.categoryId,
      },
    });

    return PrismaIncomeMapper.toDomain(income, category);
  }
}