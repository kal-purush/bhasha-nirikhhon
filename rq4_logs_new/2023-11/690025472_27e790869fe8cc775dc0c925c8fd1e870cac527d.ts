import { ApiBody, ApiOperation, ApiTags } from "@nestjs/swagger";
import { DirectLoginDto } from "./dto/\bdirectLogin.dto";
import { ApiCustomCreatedResponse } from "../common/api-response.dto";
import { LoginResponseDto } from "./dto/login-response.dto";
  @ApiOperation({ description: "직접 로그인" })
  @ApiCustomCreatedResponse(LoginResponseDto)
  @ApiBody({ type: DirectLoginDto })
import { ApiProperty } from "@nestjs/swagger";

export class DirectLoginDto {
  @ApiProperty({
    description: "이메일 또는 유저 아이디",
    example: "12345 또는 abc@naver.com",
  })
  username: string;

  @ApiProperty({ description: "비밀번호", example: "Password@1" })
  password: string;
}