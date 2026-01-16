import {
  IsBoolean,
  IsDateString,
  IsEnum,
  IsNumber,
  IsObject,
  IsOptional,
  IsString,
} from 'class-validator';
import { JSONSchema } from 'class-validator-jsonschema';
import { ApiResData, ApiResErr, ApiResPaginated } from '../types/api/api.type';
import { EnumErrorCode } from '../enums';
import httpStatus = require('http-status');

export class ApiResPaginatedDTO<TData> implements ApiResPaginated<TData> {
  @JSONSchema({ description: '請求結果' })
  @IsBoolean()
  success: boolean;

  @JSONSchema({ description: '請求狀態' })
  @IsNumber()
  status: number;

  @JSONSchema({
    description: '資料筆數',
  })
  @IsNumber()
  total: number;

  @JSONSchema({
    description: '回傳資料',
  })
  @IsObject()
  data: TData[];
}

export class ApiResDataDTO<TData> implements ApiResData<TData> {
  @JSONSchema({ description: '請求結果' })
  @IsBoolean()
  success: boolean;

  @JSONSchema({ description: '請求狀態' })
  @IsNumber()
  status: number;

  @JSONSchema({
    description: '回傳資料 T | T[] | null',
  })
  @IsObject()
  data: TData | TData[] | null;
}

export class ApiResErrDTO implements ApiResErr {
  @JSONSchema({
    description: '請求結果',
    example: false,
  })
  @IsBoolean()
  success: boolean;

  @JSONSchema({
    description: '請求狀態',
    example: httpStatus.NOT_FOUND,
  })
  @IsNumber()
  status: number;

  @JSONSchema({
    description: '錯誤代碼',
    example: EnumErrorCode.HTTP_BASE_ERROR,
  })
  @IsEnum(EnumErrorCode)
  errorCode: EnumErrorCode;

  @JSONSchema({ description: '錯誤訊息' })
  @IsString()
  message: string;

  @JSONSchema({ description: '錯誤描述(optional)' })
  @IsString()
  @IsOptional()
  description?: string;

  @JSONSchema({ description: '錯誤路徑' })
  @IsString()
  path: string;

  @JSONSchema({
    description: '錯誤時間',
    example: '2021-01-01T00:00:00.000Z',
  })
  @IsDateString()
  /* @Matches(/^\d{4}(-)(((0)[0-9])|((1)[0-2]))(-)([0-2][0-9]|(3)[0-1])$/i, {
    message: "$property must be formatted as yyyy-mm-dd"
  }) */
  timestamp: string;
}