import {
  IsInt,
  IsNumber,
  IsString,
  IsUUID,
  Max,
  MaxLength,
  Min,
  MinLength,
} from 'class-validator';

export class EditProductInfoDto {
  @IsUUID(4)
  id: string;

  @IsString()
  @MinLength(3)
  @MaxLength(25)
  name: string;

  @IsNumber()
  @Min(0.1)
  @Max(999999)
  price: number;

  @IsInt()
  @Min(1)
  @Max(5000)
  quantity: number;

  @IsString()
  @MinLength(3)
  @MaxLength(25)
  sku: string;

  @IsUUID(4)
  categoryId: string;

  @IsString()
  @MinLength(3)
  @MaxLength(1500)
  description: string;
}