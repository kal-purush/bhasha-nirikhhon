import { HttpService } from '@nestjs/axios';
import { Injectable, UnauthorizedException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

import { KakaoAuthUserResponse } from '@app/auth/auth-kakao/dto/kakao-auth-user.response';
import { UserRequestCommand } from '@app/auth/auth-kakao/kakao.command';

@Injectable()
export class AuthKakaoService {
  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
  ) {}

  async kakaoLogin(data: UserRequestCommand): Promise<KakaoAuthUserResponse> {
    const { code, domain } = data;
    const body = {
      grant_type: 'authorization_code',
      client_id: this.configService.get<string>('APP_SECRET'),
      redirect_uri: domain,
      code,
    };
    const headers = {
      'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8',
    };
    try {
      const { data: kakaoAuthData, status: kakaoAuthStatus } =
        await this.httpService.axiosRef.request({
          method: 'POST',
          url: `oauth/token`,
          timeout: 30000,
          headers,
          data: body,
        });

      if (kakaoAuthStatus === 200) {
        console.log(`kakaoToken : ${JSON.stringify(kakaoAuthData)}`);
        // Token 을 가져왔을 경우 사용자 정보 조회
        const headerUserInfo = {
          'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8',
          Authorization: 'Bearer ' + kakaoAuthData.access_token,
        };

        console.log(`headers : ${JSON.stringify(headerUserInfo)}`);
        const responseUserInfo = await this.httpService.axiosRef.request({
          method: 'GET',
          url: `v2/user/me`,
          timeout: 30000,
          headers: headerUserInfo,
        });
        if (responseUserInfo.status === 200) {
          console.log(
            `kakaoUserInfo : ${JSON.stringify(responseUserInfo.data)}`,
          );
          return new KakaoAuthUserResponse(responseUserInfo.data);
        } else {
          throw new UnauthorizedException();
        }
      } else {
        throw new UnauthorizedException();
      }
    } catch (error) {
      console.log(error);
      throw new UnauthorizedException();
    }
  }
}