import { applyDecorators } from '@nestjs/common';
import {
  ApiBadRequestResponse,
  ApiHeader,
  ApiUnauthorizedResponse,
} from '@nestjs/swagger';
import { ResponseDto } from '../src/global/dtos/response.dto';

export function RequireAccessToken() {
  return applyDecorators(
    ApiHeader({
      name: 'Authorization',
      description:
        "access token이 필요합니다 key : Authorization, value : 'Bearer ${Token}'",
      schema: {
        example: 'Authorization Bearer ${Access 토큰}',
      },
    }),
    ApiBadRequestResponse({
      description:
        'header에 토큰이 없는 경우 or token 값 자체가 이상한 경우, request body를 같이 보내는 경우 body 값 검증도 포함',
      schema: {
        example: ResponseDto.fail(400, 'token이 필요합니다.'),
      },
    }),
    ApiUnauthorizedResponse({
      description: 'access 토큰이 만료된 경우',
      schema: {
        example: ResponseDto.fail(401, '만료된 token.'),
      },
    }),
  );
}