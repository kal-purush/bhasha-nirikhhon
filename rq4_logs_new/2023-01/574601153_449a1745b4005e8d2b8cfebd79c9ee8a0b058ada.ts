import {
  BadRequestException,
  Body,
  Controller,
  Logger,
  Post,
  UnauthorizedException,
} from '@nestjs/common';
import { ApiBody, ApiOperation, ApiResponse, ApiTags } from '@nestjs/swagger';

import { AuthService } from '@app/auth/auth.service';
import { KakaoAuthRequest } from '@app/auth/dto/kakao-auth.request';
import { TokenResponse } from '@app/auth/dto/token.response';
import { UserAccountService } from '@app/user/user-account/user-account.service';
import { UserAuthType, UserRole } from '@domain/user/user';

@Controller('auth')
@ApiTags('[인증] 계정')
export class AuthController {
  private readonly logger = new Logger(AuthController.name);
  constructor(
    private readonly authService: AuthService,
    private readonly userAccountService: UserAccountService,
  ) {}

  @Post('login/kakao')
  @ApiOperation({ summary: '카카오 로그인' })
  @ApiBody({ type: KakaoAuthRequest })
  @ApiResponse({
    type: TokenResponse,
  })
  async kakaoLogin(
    @Body() accountRequestInfo: KakaoAuthRequest,
  ): Promise<TokenResponse> {
    try {
      // 카카오 토큰 조회 후 계정 정보 가져오기
      const { code, domain } = accountRequestInfo;
      if (!code || !domain) {
        throw new BadRequestException('카카오 정보가 없습니다.');
      }
      const kakao = await this.authService.kakaoLogin({ code, domain });

      if (!kakao.id) {
        throw new BadRequestException('카카오 정보가 없습니다.');
      }

      const user = await this.userAccountService.findByUsername(
        kakao.id.toString(),
      );

      if (!user) {
        const user = await this.userAccountService.joinUser({
          username: kakao.id.toString(),
          email: kakao.kakao_account.email,
          role: UserRole.USER,
          authType: UserAuthType.KAKAO,
        });

        return await this.authService.login(user.username, UserAuthType.KAKAO);
      }

      return await this.authService.login(
        kakao.id.toString(),
        UserAuthType.KAKAO,
      );
    } catch (e) {
      this.logger.error(e);
      throw new UnauthorizedException('카카오 로그인에 실패했습니다.');
    }
  }
}