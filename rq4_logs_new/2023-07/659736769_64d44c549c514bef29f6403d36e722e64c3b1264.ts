import {
  Res,
  Get,
  Body,
  Post,
  Query,
  Patch,
  Param,
  Request,
  UseGuards,
  Controller,
} from '@nestjs/common';
import { Response } from 'express';
import { CreatePlanningBody } from '../dtos/planning/create-planning-body';
import { JwtGuard } from '@application/use-cases/auth/guards/jwt-auth.guard';
import { CreatePlanning } from '@application/use-cases/planning/create-planning';

@UseGuards(JwtGuard)
@Controller('plannings')
export class PlanningController {
  constructor(private createPlanning: CreatePlanning) {}

  @Post('create-planning')
  async create(
    @Body() bodyRequest: CreatePlanningBody,
    @Request() req,
    @Res()
    response: Response,
  ) {
    const { user_uuid } = req.user;

    await this.createPlanning.execute({ user_uuid, ...bodyRequest });

    response.json({
      status: 201,
      message: 'Planejamento cadastrado com sucesso!',
    });
  }
}