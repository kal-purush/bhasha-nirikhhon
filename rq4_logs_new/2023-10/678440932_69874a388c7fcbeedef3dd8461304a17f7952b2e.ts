/* eslint-disable prettier/prettier */

import { IsNotEmpty, IsString } from "class-validator";

export class CreateCityDto {
    @IsString()
    @IsNotEmpty()
    readonly city: string;
}