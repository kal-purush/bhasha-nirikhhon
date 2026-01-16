import { Module } from "@nestjs/common";
import { AuthSocialController } from "./controllers/auth-social.controller";
import { AuthSocialService } from "./service/auth-social.service";
import { UsersModule } from "@src/apis/users/users.module";
import { AuthRegistrationService } from "./service/auth-registration.service";
import { AuthService } from "../services/auth.service";
import { EncryptionModule } from "@src/libs/encryption/encryption.module";
import { JwtModule } from "@nestjs/jwt";
import { JwtModuleOptionsFactory } from "../jwt/jwt-module-options.factory";

@Module({
  imports: [
    UsersModule,
    EncryptionModule,
    JwtModule.registerAsync({
      useClass: JwtModuleOptionsFactory,
    }),
  ],
  controllers: [AuthSocialController],
  providers: [AuthSocialService, AuthRegistrationService, AuthService],
  exports: [AuthSocialService]
})

export class AuthSocialModule { }