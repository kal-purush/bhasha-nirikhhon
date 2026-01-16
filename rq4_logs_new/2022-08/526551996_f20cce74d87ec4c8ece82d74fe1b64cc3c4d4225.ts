import { Module } from '@nestjs/common';
import { AccountController } from './account/account.controller';
import { HttpModule } from '@nestjs/axios';

@Module({
  controllers: [AccountController],
  imports: [HttpModule],
})
export class AccountModule {}