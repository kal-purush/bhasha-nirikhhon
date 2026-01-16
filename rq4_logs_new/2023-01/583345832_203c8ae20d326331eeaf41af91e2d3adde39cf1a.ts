import {
  IsBoolean,
  IsNotEmpty,
  IsNumber,
  IsOptional,
  IsString,
  Matches,
  MaxLength,
  MinLength,
} from 'class-validator';

export class CreateUserDto {
  @IsNotEmpty({ message: '아이디를 입력해주세요' })
  @IsString()
  @MinLength(1)
  @MaxLength(20)
  Id: string;

  @IsNotEmpty({ message: '비밀번호를 입력해 주세요' })
  @IsString()
  @MinLength(1)
  @MaxLength(20)
  @Matches(/^[a-zA-z0-9]*$/, {
    message: '비번은 영문이나 숫자 1~20자 입니다.',
  })
  password: string;

  @IsOptional()
  @IsString()
  @MinLength(1)
  @MaxLength(20)
  name: string;
}