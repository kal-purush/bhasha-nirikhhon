import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';

import { UserAccountController } from '@app/user/user-account/user-account.controller';
import { UserAccountService } from '@app/user/user-account/user-account.service';
import { User } from '@domain/user/user.entity';

@Module({
  imports: [TypeOrmModule.forFeature([User])],
  controllers: [UserAccountController],
  providers: [UserAccountService],
  exports: [UserAccountService],
})
export class UserAccountModule {}