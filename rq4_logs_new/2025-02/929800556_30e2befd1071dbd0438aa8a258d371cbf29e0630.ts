// src/cron/cron.module.ts
import { Module } from '@nestjs/common';
import { ScheduleModule } from '@nestjs/schedule';
import { CronService } from './cron.service';

@Module({
  imports: [ScheduleModule.forRoot()], // Initialize the ScheduleModule
  providers: [CronService],            // Register the service
})
export class CronModule {}