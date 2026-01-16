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
import { CreateCategoryBody } from '../dtos/category/create-category-body';
import { UpdateCategoryBody } from '../dtos/category/update-category-body';
import { JwtGuard } from '@application/use-cases/auth/guards/jwt-auth.guard';
import { CreateCategory } from '@application/use-cases/category/create-category';
import { UpdateCategoryById } from '@application/use-cases/category/update-category-by-id';
import { GetAllCategories } from '@application/use-cases/category/get-all-categories';

@Controller('category')
export class CategoryController {
  constructor(
    private createCategory: CreateCategory,
    private updateCategoryById: UpdateCategoryById,
    private getAllCategories: GetAllCategories,
  ) {}

  @UseGuards(JwtGuard)
  @Post('create-category')
  async create(
    @Body() bodyRequest: CreateCategoryBody,
    @Request() req,
    @Res()
    response: Response,
  ): Promise<void> {
    const { user_uuid } = req.user;

    await this.createCategory.execute({ user_uuid, ...bodyRequest });

    response.json({ status: 201, message: 'Categoria criada com sucesso!' });
  }

  @UseGuards(JwtGuard)
  @Post('update-category/:id')
  async update(
    @Param() param,
    @Body() bodyRequest: UpdateCategoryBody,
    @Res()
    response: Response,
  ): Promise<void> {
    const category_uuid = param.id;

    await this.updateCategoryById.execute({ category_uuid, ...bodyRequest });

    response.json({
      status: 201,
      message: 'Categoria atualizada com sucesso!',
    });
  }

  @UseGuards(JwtGuard)
  @Get('get-all-categories')
  async returnAllCategories(@Request() req) {
    const { user_uuid } = req.user;

    const { allCategories, incomeCategories, expenseCategories } =
      await this.getAllCategories.execute({ user_uuid });

    return {
      allCategories,
      incomeCategories,
      expenseCategories,
    };
  }
}