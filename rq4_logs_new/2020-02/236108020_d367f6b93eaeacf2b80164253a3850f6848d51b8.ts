import { Column } from 'typeorm';
import { IsDate, IsDateString, IsNotEmpty, IsNumberString, IsString, MaxLength, Min, MinLength } from 'class-validator';

const f=new Date();
export class CabCarritoCreateDto {


  @IsNotEmpty()
  @IsString()
  @MinLength(6)
  @MaxLength(8)
  estado:string="Creado";


  @IsNotEmpty()
  // @IsDate()
  fecha:string=`${f.getFullYear()}/${f.getMonth()}/${f.getDate()}`;

  @IsNotEmpty()
  @Min(0)
  total:number;

  @IsNotEmpty()
  @MinLength(3)
  @MaxLength(50)
  direccion:string;
}