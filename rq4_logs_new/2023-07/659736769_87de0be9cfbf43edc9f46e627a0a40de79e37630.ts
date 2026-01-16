import { WalletRepository } from '@application/repositories/wallet-repository';
import { Injectable } from '@nestjs/common';
import { FindWalletByUserId } from './find-wallet-by-user-id';
import { Wallet } from '@application/entities/wallet.entity';

interface UpdateWalletByIdRequest {
  user_uuid: string;
  value?: number;
}

@Injectable()
export class UpdateWalletById {
  constructor(
    private walletRepository: WalletRepository,
    private findWalletByUserId: FindWalletByUserId,
  ) {}

  async execute({ user_uuid, value }: UpdateWalletByIdRequest): Promise<void> {
    const { wallet: currentWallet } = await this.findWalletByUserId.execute({
      user_uuid,
    });

    const updatedWallet = new Wallet(
      {
        value: value ?? currentWallet.value,
        createdAt: currentWallet.createdAt,
      },
      currentWallet.id,
    );

    await this.walletRepository.update(updatedWallet);
  }
}