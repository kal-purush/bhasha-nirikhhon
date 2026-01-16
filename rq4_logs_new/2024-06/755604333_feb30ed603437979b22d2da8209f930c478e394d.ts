import { Injectable } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { CronRepository } from './cron.repository';

/**
 * Service for handling cron jobs.
 */
@Injectable()
export class CronService {
  constructor(private readonly cronRepository: CronRepository) {}

  /**
   * Cleanup expired reset tokens.
   *
   * Runs every day at 1AM.
   *
   * For example, when a user requests a reset but never completes flow.
   */
  @Cron(CronExpression.EVERY_DAY_AT_1AM)
  async cleanupExpiredTokens() {
    await this.cronRepository.cleanupExpiredTokens();
  }
}