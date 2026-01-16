import { IsString, IsNotEmpty, IsNumber, MinLength, IsArray, IsOptional } from "class-validator";

export class CreateShirtDto {
    @IsString()
    @IsNotEmpty()
    marca: string;

    @IsString()
    @IsNotEmpty()
    color: string;

    @IsNumber()
    @IsNotEmpty()
    precio: number;

    @IsString({ each: true })
    @IsArray()
    @IsOptional()
    sizes?: string[];
}