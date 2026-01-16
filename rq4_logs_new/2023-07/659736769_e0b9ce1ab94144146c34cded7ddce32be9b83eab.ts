import { Wallet } from '@application/entities/wallet.entity';
import { WalletRepository } from '@application/repositories/wallet-repository';
import { HttpException, Injectable } from '@nestjs/common';

interface FindWalletByUserIdRequest {
  user_uuid: string;
}

interface FindWalletByUserIdResponse {
  wallet: Wallet;
}

@Injectable()
export class FindWalletByUserId {
  constructor(private walletRepository: WalletRepository) {}

  async execute({
    user_uuid,
  }: FindWalletByUserIdRequest): Promise<FindWalletByUserIdResponse> {
    const wallet = await this.walletRepository.findWalletByUserId(user_uuid);
    console.log(wallet);
    if (!wallet) {
      throw new HttpException('A carteira do usuário não foi encontrada!', 404);
    }

    return { wallet };
  }
}