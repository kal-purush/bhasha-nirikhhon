import { DescriptionAndOptions } from "@nestjs/common";
import { IsString, IsNumber, IsNotEmpty } from "class-validator";

export class createProductsDto {
    @IsString()
    @IsNotEmpty()
    categoria: string;

    @IsString()
    @IsNotEmpty()
    producto: string;

    @IsString()
    @IsNotEmpty()
    referencia: string;
    
    @IsNumber()
    @IsNotEmpty()
    tamaño: number;
    
    @IsString()
    @IsNotEmpty()
    medida: string;
    
    @IsNumber()
    @IsNotEmpty()
    cantidad: number;

    @IsNumber()
    @IsNotEmpty()
    precio: number;
    
    @IsNumber()
    @IsNotEmpty()
    total: number;
}