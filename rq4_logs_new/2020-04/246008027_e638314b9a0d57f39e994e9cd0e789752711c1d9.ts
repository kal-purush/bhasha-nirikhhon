import { RoleEnum } from "../../server/modules/db/domain/role.enum";
import { UserStatusEnum } from "../../server/modules/db/domain/user-status.enum";
import {
    IsBoolean,
    IsEmail,
    IsEnum,
    IsNumber,
    IsOptional,
    IsString,
    Length
} from "class-validator";

export class CreateUserDto {
    @IsOptional()
    @IsEmail()
    email?: string;

    @Length(3, 20)
    nickname: string;

    @IsOptional()
    @Length(5, 20)
    password?: string;

    @IsOptional()
    @IsString()
    avatarUrl?: string;

    @IsOptional()
    @IsEnum(RoleEnum)
    role?: RoleEnum;

    @IsOptional()
    @IsNumber()
    balance?: number;

    @IsOptional()
    @IsNumber()
    experience?: number;

    @IsOptional()
    @IsBoolean()
    isAnon?: boolean;

    @IsOptional()
    @IsEnum(UserStatusEnum)
    status?: UserStatusEnum;
}