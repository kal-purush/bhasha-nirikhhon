import { Module } from '@nestjs/common';
import { AuthController } from '../controllers/auth.controller';
import { AuthService } from '../services/auth.service';
import { UsersModule } from 'src/users/modules/users.module';
import { jwtConstants } from '../../constants/constants';
import { JwtModule } from '@nestjs/jwt';
import { AuthGuard } from '../guards/auth.guard';
import { PassportModule } from '@nestjs/passport';
import { LocalStrategy } from '../stategies/local.strategy';

@Module({
  imports: [
    UsersModule,
    JwtModule.register({
      global: true,
      secret: jwtConstants.secret,
      signOptions: { expiresIn: '15m' },
    }),
    PassportModule,
  ],
  controllers: [AuthController],
  providers: [
    AuthService,
    {
      provide: 'APP_GUARD',
      useClass: AuthGuard,
    },
    LocalStrategy,
  ],
})
export class AuthModule {}