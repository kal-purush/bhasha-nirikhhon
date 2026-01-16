import {
  Body,
  Controller,
  Get,
  Param,
  Post,
  Request,
  Res,
  UseGuards,
} from '@nestjs/common';
import { Response } from 'express';
import { IncomeViewModel } from '../view-models/income-view-model';
import { CreateIncomeBody } from '../dtos/income/create-income-body';
import { CreateIncome } from '@application/use-cases/income/create-income';
import { JwtGuard } from '@application/use-cases/auth/guards/jwt-auth.guard';
import { FindIncomeById } from '@application/use-cases/income/find-income-by-id';
import { UpdateIncomeById } from '@application/use-cases/income/update-income-by-id';

@UseGuards(JwtGuard)
@Controller('incomes')
export class IncomeController {
  constructor(
    private createIncome: CreateIncome,
    private findIncomeById: FindIncomeById,
    private updateIncomeById: UpdateIncomeById,
  ) {}

  @Post('create-income')
  async create(
    @Body() bodyRequest: CreateIncomeBody,
    @Request() req,
    @Res()
    response: Response,
  ) {
    const { user_uuid } = req.user;

    await this.createIncome.execute({ user_uuid, ...bodyRequest });

    response.json({ status: 201, message: 'Receita cadastrada com sucesso!' });
  }

  @Get('find-income-by/:id')
  async findById(@Param() param) {
    const income_uuid = param.id;

    const { income } = await this.findIncomeById.execute({ income_uuid });

    return IncomeViewModel.toHTTP(income);
  }
}