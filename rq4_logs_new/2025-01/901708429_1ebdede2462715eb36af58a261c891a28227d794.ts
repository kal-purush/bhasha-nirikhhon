import { Module } from '@nestjs/common';
import { AuthService } from '@/auth/auth.service';
import { AuthController } from '@/auth/auth.controller';
import { JwtModule } from '@nestjs/jwt';
import { env } from '@/config/env.config';
import { UsersModule } from '@/users/users.module';

@Module({
	imports: [
		UsersModule,
		JwtModule.register({
			global: true,
			secret: env.JWT_SECRET,
			signOptions: { expiresIn: `${env.JWT_EXPIRATION}` },
		}),
	],
	controllers: [AuthController],
	providers: [AuthService],
})
export class AuthModule {}