import {
  Controller,
  Get,
  Inject,
  Body,
  Delete,
  Param,
  Post,
  Put,
} from '@nestjs/common';
import { ExpenseService } from './expense.service';
import {
  CreateExpenseResponse,
  GetListOfExpensesResponse,
  GetOneExpenseResponse,
  UpdateExpenseResponse,
} from '../interfaces/expense';
import { Expense } from './expense.entity';
import { GetShortListOfExpenseResponse } from '../interfaces/short-expense';

@Controller('expense')
export class ExpenseController {
  constructor(@Inject(ExpenseService) private expenseService: ExpenseService) {}

  @Get('/')
  getListOfExpenses(): Promise<GetListOfExpensesResponse> {
    return this.expenseService.getExpenses();
  }

  @Get('/short-list')
  getShortListOfExpenses(): Promise<GetShortListOfExpenseResponse> {
    return this.expenseService.getShortListOfExpenses();
  }

  @Get('/:id')
  getOneExpense(@Param('id') id: string): Promise<GetOneExpenseResponse> {
    return this.expenseService.getOneExpense(id);
  }

  @Delete('/:id')
  removeExpense(@Param('id') id: string): Promise<void> {
    return this.expenseService.removeExpense(id);
  }

  @Post('/')
  createExpense(@Body() newExpense: Expense): Promise<CreateExpenseResponse> {
    return this.expenseService.createExpense(newExpense);
  }

  @Put('/:id')
  updateExpense(
    @Param('id') id: string,
    @Body() updatedExpense: Expense,
  ): Promise<UpdateExpenseResponse> {
    return this.expenseService.updateExpense(id, updatedExpense);
  }
}