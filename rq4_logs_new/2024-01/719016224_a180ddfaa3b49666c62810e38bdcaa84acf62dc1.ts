import { ApiProperty } from '@nestjs/swagger';
import { AUTH_ERROR_CODE } from '@src/constants/error/auth/auth-error-code.constant';
import { AUTH_ERROR_MESSAGE } from '@src/constants/error/auth/auth-error-message.constant';
import { COMMON_ERROR_CODE } from '@src/constants/error/common/common-error-code.constant';
import { COMMON_ERROR_MESSAGE } from '@src/constants/error/common/common-error-message.constant';
import { MAJOR_ERROR_CODE } from '@src/constants/error/major/major-error-code.constant';
import { MAJOR_ERROR_MESSAGE } from '@src/constants/error/major/major-error-message.constant';
import { REACTION_ERROR_CODE } from '@src/constants/error/reaction/reaction-error-code.constant';
import { REACTION_ERROR_MESSAGE } from '@src/constants/error/reaction/reaction-error-message.constant';
import { USER_ERROR_CODE } from '@src/constants/error/users/user-error-code.constant';
import { USER_ERROR_MESSAGE } from '@src/constants/error/users/user-error-message.constant';
import { ErrorMessage } from '@src/types/type';

const buildErrorExample = <T extends Record<string, number>>(
  codeMap: T,
  messageMap: ErrorMessage<T>,
) => {
  return Object.values(codeMap).map((code) => {
    return {
      code,
      message: messageMap[code],
    };
  });
};

const buildErrorProperty = <T extends Record<string, number>>(
  codeMap: T,
  messageMap: ErrorMessage<T>,
) => {
  return {
    type: 'array',
    items: {
      properties: {
        code: {
          type: 'enum',
          enum: Object.values(codeMap),
        },
        message: {
          type: 'enum',
          enum: Object.values(messageMap),
        },
      },
      required: ['code', 'message'],
    },
    example: buildErrorExample(codeMap, messageMap),
  };
};

export class ErrorCodeResponseDto {
  @ApiProperty(buildErrorProperty(COMMON_ERROR_CODE, COMMON_ERROR_MESSAGE))
  common = buildErrorExample(COMMON_ERROR_CODE, COMMON_ERROR_MESSAGE);

  @ApiProperty(buildErrorProperty(AUTH_ERROR_CODE, AUTH_ERROR_MESSAGE))
  auth = buildErrorExample(AUTH_ERROR_CODE, AUTH_ERROR_MESSAGE);

  @ApiProperty(buildErrorProperty(USER_ERROR_CODE, USER_ERROR_MESSAGE))
  user = buildErrorExample(USER_ERROR_CODE, USER_ERROR_MESSAGE);

  @ApiProperty(buildErrorProperty(MAJOR_ERROR_CODE, MAJOR_ERROR_MESSAGE))
  major = buildErrorExample(MAJOR_ERROR_CODE, MAJOR_ERROR_MESSAGE);

  @ApiProperty(buildErrorProperty(REACTION_ERROR_CODE, REACTION_ERROR_MESSAGE))
  reaction = buildErrorExample(REACTION_ERROR_CODE, REACTION_ERROR_MESSAGE);
}