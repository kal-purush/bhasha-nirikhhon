import { Column, Index } from 'typeorm';
import { IsNotEmpty, MaxLength, Min, MinLength } from 'class-validator';

export class ProductoCreateDto {
  @IsNotEmpty()
  @MinLength(5)
  @MaxLength(50)
  nombre:string;

  @IsNotEmpty()
  @MinLength(5)
  @MaxLength(100)
  descripcion:string;

  @IsNotEmpty()
  @Min(0)
  costo:number;
}