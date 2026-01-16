import { Injectable } from '@nestjs/common';

import { BalanceCreatorDto } from '@/contexts/balances/application/balance-creator/balance-creator.dto';
import { Balance } from '@/contexts/balances/domain/balance.entity';
import { BalanceRepository } from '@/contexts/balances/domain/balance.repository';
import { BalanceUpsertedEvent } from '@/shared/domain/events/balance-upserted.event';
import { EventDispatcher } from '@/shared/domain/events/event-dispatcher';

@Injectable()
export class BalanceCreator {
  constructor(
    private readonly balanceRepository: BalanceRepository,
    private readonly eventDispatcher: EventDispatcher,
  ) {}

  async execute(balanceCreatorDto: BalanceCreatorDto): Promise<Balance[]> {
    const userId = balanceCreatorDto[0].userId;

    const balances = balanceCreatorDto.map((balanceDto) => {
      return new Balance(
        balanceDto.id,
        balanceDto.name,
        balanceDto.amount,
        balanceDto.amount, // Initially the usable amount is the same as the amount
        balanceDto.userId,
        balanceDto.color,
      );
    });

    await this.balanceRepository.save(balances);

    const dbBalances = await this.balanceRepository.findByUserId(userId);

    const totalBalancesAmount = dbBalances.reduce(
      (total, balance) => total + balance.amount,
      0,
    );

    await this.eventDispatcher.dispatch(
      new BalanceUpsertedEvent(userId, totalBalancesAmount),
    );

    return balances;
  }
}