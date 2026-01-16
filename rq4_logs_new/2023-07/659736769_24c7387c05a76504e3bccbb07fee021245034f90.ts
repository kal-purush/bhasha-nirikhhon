import { Module } from '@nestjs/common';
import { IncomeController } from '../controllers/income.controller';
import { DatabaseModule } from '@infra/database/prisma/database.module';
import { CreateIncome } from '@application/use-cases/income/create-income';
import { FindIncomeById } from '@application/use-cases/income/find-income-by-id';
import { UpdateIncomeById } from '@application/use-cases/income/update-income-by-id';

@Module({
  imports: [DatabaseModule],
  controllers: [IncomeController],
  providers: [CreateIncome, FindIncomeById, UpdateIncomeById],
})
export class IncomeModule {}