
// ================================================================>> Core Library
import { Optional } from '@nestjs/common';
import { IsNotEmpty, IsNumber, IsString } from 'class-validator';

// ================================================================>> Costom Library

export class CreateUserDTO {

    @IsString()
    @IsNotEmpty()
    name: string;

    @IsString()
    @IsNotEmpty()
    email: string;

    @IsString()
    @IsNotEmpty()
    phone: string;

    @Optional()
    phone2: string;

    @IsString()
    @IsNotEmpty()
    password: string = '123456';
    
    @IsNumber()
    @IsNotEmpty()
    role_id: number;

    @IsString()
    @Optional()
    avatar: string = 'static/ecommerce/user/avatar.png';

    @IsNumber()
    @IsNotEmpty()
    is_active: number = 1;
}