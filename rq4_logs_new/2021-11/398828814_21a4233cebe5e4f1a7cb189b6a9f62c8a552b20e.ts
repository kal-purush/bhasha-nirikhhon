import {
  Body,
  Controller,
  Get,
  Param,
  Patch,
  Post,
  UseGuards,
} from '@nestjs/common';
import { PositionService } from './position.service';
import { JwtAuthGuard } from '../../auth/jwt-auth.guard';
import { CreatePositionDto } from '../../types/positions/CreatePositionDto';
import { Position } from './position.schema';
import * as mongoose from 'mongoose';
import { UpdatePositionDto } from '../../types/positions/UpdatePositionDto';

@Controller('positions')
export class PositionController {
  constructor(private positionService: PositionService) {}

  @UseGuards(JwtAuthGuard)
  @Get(':positionId')
  async getPositionById(@Param('positionId') positionId: string) {
    return this.positionService.findPositionById(positionId);
  }

  @UseGuards(JwtAuthGuard)
  @Get('provider/:providerId')
  async getPositionByProviderId(
    @Param('providerId') providerId: mongoose.Types.ObjectId,
  ) {
    return this.positionService.findPositionByProviderId(providerId);
  }

  @UseGuards(JwtAuthGuard)
  @Post()
  async createPosition(
    @Body() createPositionDto: CreatePositionDto,
  ): Promise<Position> {
    return this.positionService.createPosition(createPositionDto);
  }

  @UseGuards(JwtAuthGuard)
  @Patch(':positionId')
  async updatePositionById(
    @Param('positionId') positionId: mongoose.Types.ObjectId,
    @Body() updatePositionDto: UpdatePositionDto,
  ): Promise<Position> {
    return this.positionService.updatePositionById(
      positionId,
      updatePositionDto,
    );
  }

  @UseGuards(JwtAuthGuard)
  @Patch('provider/:providerId')
  async updatePositionByProvider(
    @Param('providerId') providerId: mongoose.Types.ObjectId,
    @Body() updatePositionDto: UpdatePositionDto,
  ): Promise<Position> {
    return this.positionService.updatePositionByProviderId(
      providerId,
      updatePositionDto,
    );
  }
}