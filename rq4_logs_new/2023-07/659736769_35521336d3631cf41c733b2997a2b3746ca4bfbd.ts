import { UserAuth } from '@application/use-cases/auth/auth-user';
import { DatabaseModule } from '@infra/database/prisma/database.module';
import { Module } from '@nestjs/common';
import { UserAuthController } from '../controllers/auth.controller';
import { JwtModule } from '@nestjs/jwt';
import { UserAuthLogin } from '@application/use-cases/auth/auth-login';
import { LocalStrategy } from '@application/use-cases/auth/strategies/local.strategy';
import { JwtStrategy } from '@application/use-cases/auth/strategies/jwt.strategy';
import { AuthRefreshToken } from '@application/use-cases/auth/auth-refresh-token';
import { RefreshJwtStrategy } from '@application/use-cases/auth/strategies/refreshToken.strategy';

@Module({
  providers: [
    UserAuth,
    UserAuthLogin,
    AuthRefreshToken,
    LocalStrategy,
    JwtStrategy,
    RefreshJwtStrategy,
  ],
  controllers: [UserAuthController],
  imports: [
    DatabaseModule,
    JwtModule.register({
      secret: process.env.SECRET_KEY,
      signOptions: { expiresIn: '60s' },
    }),
  ],
})
export class UserAuthModule {}