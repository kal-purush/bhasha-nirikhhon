import { Body, Controller, Post } from "@nestjs/common";
import { ApiTags } from "@nestjs/swagger";
import { AuthSocialService } from "../service/auth-social.service";
import { SignInRequestBodyDto, SignUpRequestBodyDto } from "../dto/auth-social.dto";
import { CheckRegistrationRequestBodyDto } from "../dto/auth-registration.dto";
import { AuthRegistrationService } from "../service/auth-registration.service";
import { ApiAuthSocial } from "./auth-social.swagger";

@ApiTags('auth/social')
@Controller('auth/social')
export class AuthSocialController {
  constructor(
    private readonly authSocialService: AuthSocialService,
    private readonly authRegistrationService: AuthRegistrationService
  ) { }

  @ApiAuthSocial.CheckRegistration({ summary: '소셜 유저 프로필 유무 조회' })
  @Post('check-registration')
  async checkRegistration(@Body() checkRegistrationRequestBodyDto: CheckRegistrationRequestBodyDto): Promise<boolean> {
    return await this.authRegistrationService.checkUserRegistered(checkRegistrationRequestBodyDto)
  }

  @ApiAuthSocial.SignUp({ summary: '소셜 회원가입' })
  @Post('signup')
  async signUp(@Body() signUpRequestBodyDto: SignUpRequestBodyDto) {
    return await this.authSocialService.signUp(signUpRequestBodyDto);
  }

  @ApiAuthSocial.SignIn({ summary: '소셜 로그인' })
  @Post('signin')
  async signIn(@Body() signInRequestBodyDto: SignInRequestBodyDto) {
    return await this.authSocialService.signIn(signInRequestBodyDto);
  }
}