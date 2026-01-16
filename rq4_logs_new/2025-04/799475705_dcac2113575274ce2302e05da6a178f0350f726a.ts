import { Injectable, Inject, OnApplicationBootstrap } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { ProtectionResultDto } from '../protection-rules/dto';
import { PROTECTION_RESULT_KEY } from '../protection-rules/protection-rules.constants';
import { LogType } from '../logs/enums';
import { AsicsRepository } from './asics.repository';
import { CACHE_MANAGER } from '@nestjs/cache-manager';
import { Cache } from 'cache-manager';
import { AsicsService } from './asics.service';
import { ControlRuleService } from '../automation/control-rule/control-rule.service';
import { AsicsScalingStrategyExecutor } from './strategies';
import { OnEvent } from '@nestjs/event-emitter';
import { CONTROL_RULE_SAVED_EVENT } from '../automation/control-rule/control-rule.constants';
import { ControlRule } from '../automation/control-rule/entities';

@Injectable()
export class AsicsAutomationService implements OnApplicationBootstrap {
  constructor(
    @Inject(CACHE_MANAGER)
    private readonly cache: Cache,
    private readonly asicsRepository: AsicsRepository,
    private readonly asicsService: AsicsService,
    private readonly controlRuleService: ControlRuleService,
    private readonly asicScalingStrategyExecutor: AsicsScalingStrategyExecutor,
  ) {}

  @Cron(CronExpression.EVERY_DAY_AT_11PM)
  async handleStartAsicsCron(): Promise<void> {
    const protection = await this.cache.get<ProtectionResultDto>(
      PROTECTION_RESULT_KEY,
    );

    if (protection?.triggered) {
      return;
    }

    const asics = await this.asicsRepository.findWhere({ t2Active: true });

    await Promise.allSettled(
      this.asicsService.startAsics(asics, LogType.CONTROL),
    );
  }

  @Cron(CronExpression.EVERY_DAY_AT_7AM)
  async handleStopAsicsCron(): Promise<void> {
    const protection = await this.cache.get<ProtectionResultDto>(
      PROTECTION_RESULT_KEY,
    );

    if (protection?.triggered) {
      return;
    }

    const asics = await this.asicsRepository.findWhere({ t2EndStop: true });

    await Promise.allSettled(
      this.asicsService.stopAsics(asics, LogType.CONTROL),
    );
  }

  @OnEvent(CONTROL_RULE_SAVED_EVENT)
  restartAsicScalingStrategies(rule: ControlRule): void {
    this.asicScalingStrategyExecutor.execute(rule);
  }

  async onApplicationBootstrap(): Promise<void> {
    const rules = await this.controlRuleService.getRules();

    this.asicScalingStrategyExecutor.execute(rules);
  }
}