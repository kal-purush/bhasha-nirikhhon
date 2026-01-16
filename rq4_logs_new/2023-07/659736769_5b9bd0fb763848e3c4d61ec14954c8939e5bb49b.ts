import { JwtGuard } from '@application/use-cases/auth/guards/jwt-auth.guard';
import { UpdateWalletById } from '@application/use-cases/wallet/update-wallet-by-id';
import {
  Body,
  Controller,
  Patch,
  Res,
  UseGuards,
  Request,
} from '@nestjs/common';
import { UpdateWalletBody } from '../dtos/wallet/update-wallet-body';
import { Response } from 'express';

@Controller('wallet')
export class WalletController {
  constructor(private updateWalletById: UpdateWalletById) {}

  @UseGuards(JwtGuard)
  @Patch('update/')
  async update(
    @Request() req,
    @Res()
    response: Response,
    @Body() updateWalletBody: UpdateWalletBody,
  ) {
    const { user_uuid } = req.user;

    await this.updateWalletById.execute({
      user_uuid,
      ...updateWalletBody,
    });

    response.json({
      status: 200,
      message: 'Sua carteira foi atualizada com sucesso!',
    });
  }
}