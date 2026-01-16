import { IsString } from 'class-validator';

export class AddUserDto {

    @IsString()
    firstName: string;

    @IsString()
    lastName: string;
}