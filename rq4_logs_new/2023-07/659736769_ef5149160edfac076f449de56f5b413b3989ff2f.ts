import { WalletRepository } from '@application/repositories/wallet-repository';

import { PrismaWalletMapper } from '../mappers/prisma-wallet-mapper';
import { Wallet } from '@application/entities/wallet.entity';
import { PrismaService } from '@infra/database/prisma.service';
import { Injectable } from '@nestjs/common';

@Injectable()
export class PrismaWalletRepository implements WalletRepository {
  constructor(private prisma: PrismaService) {}

  async update(wallet: Wallet): Promise<void> {
    const walletRaw = PrismaWalletMapper.toPrisma(wallet);

    await this.prisma.wallet.update({
      where: {
        id: walletRaw.id,
      },
      data: walletRaw,
    });
  }

  async findWalletByUserId(userId: string): Promise<Wallet | null> {
    const wallet = await this.prisma.wallet.findUniqueOrThrow({
      where: {
        userId: userId,
      },
    });

    if (!wallet) {
      return null;
    }

    return PrismaWalletMapper.toDomain(wallet);
  }
}