import { Module } from '@nestjs/common';
import { ProtectionRuleService } from './protection-rule.service';
import { ProtectionRuleController } from './protection-rule.controller';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ProtectionRule } from './entities';
import { ProtectionRuleRepository } from './protection-rule.repository';
import { EspApiModule } from '@api/modules/esp';
import { PROTECTION_STRATEGY_CONFIG } from './protection-rule.constants';
import {
  DcBatteryProtectionStrategy,
  AcOutputProtectionStrategy,
  ProtectionStrategy,
  ProtectionStrategyExecutor,
} from './strategies';
import { LogsModule } from '../../logs/logs.module';
import { AsicsModule } from '../../asics/asics.module';
import { ProtectionStrategyConfig } from './protection-rule.types';

const PROTECTION_STRATEGIES = [
  AcOutputProtectionStrategy,
  DcBatteryProtectionStrategy,
];

@Module({
  imports: [
    TypeOrmModule.forFeature([ProtectionRule]),
    EspApiModule,
    LogsModule,
    AsicsModule,
  ],
  controllers: [ProtectionRuleController],
  providers: [
    ProtectionRuleService,
    ProtectionRuleRepository,
    ProtectionStrategyExecutor,
    {
      provide: PROTECTION_STRATEGY_CONFIG,
      useFactory: (
        ...strategies: ProtectionStrategy[]
      ): ProtectionStrategyConfig => {
        return strategies.reduce((acc, strategy) => {
          acc[strategy.name] = strategy;

          return acc;
        }, {} as ProtectionStrategyConfig);
      },
      inject: PROTECTION_STRATEGIES,
    },
    ...PROTECTION_STRATEGIES,
  ],
})
export class ProtectionRuleModule {}