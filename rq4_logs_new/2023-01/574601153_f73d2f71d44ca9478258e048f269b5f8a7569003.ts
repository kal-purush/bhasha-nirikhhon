import { ApiProperty } from '@nestjs/swagger';
import { IsNotEmpty, IsNumber, IsOptional, IsString } from 'class-validator';

export class KakaoAuthTokenResponse {
  @ApiProperty({ description: '토큰 타입, bearer로 고정' })
  @IsString()
  @IsNotEmpty()
  token_type: string;

  @ApiProperty({ description: '사용자 액세스 토큰 값' })
  @IsString()
  @IsNotEmpty()
  access_token: string;

  @ApiProperty({ description: 'ID 토큰 값' })
  @IsString()
  @IsOptional()
  id_token?: string;

  @ApiProperty({ description: '액세스 토큰과 ID 토큰의 만료 시간(초)' })
  @IsNumber()
  @IsNotEmpty()
  expires_in: number;

  @ApiProperty({ description: '사용자 리프레시 토큰 값' })
  @IsString()
  @IsNotEmpty()
  refresh_token: string;

  @ApiProperty({ description: '리프레시 토큰의 만료 시간(초)' })
  @IsNumber()
  @IsNotEmpty()
  refresh_token_expires_in: number;

  @ApiProperty({ description: '인증된 사용자의 정보 조회 권한 범위' })
  @IsString()
  @IsOptional()
  scope?: string;

  constructor(props) {
    Object.assign(this, props);
  }
}