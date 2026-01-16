import { IsNumberString, IsString, MinLength } from 'class-validator';
import {
  IsCurrentPriceLessThanOrEqualBasePrice,
  IsValidCategoryId,
} from '../../decorators';
import {
  DESCRIPTION_MIN_LENGTH,
  Error,
  NAME_MIN_LENGTH,
} from '../../constants';

export class CreateProductDto {
  @IsString()
  @MinLength(NAME_MIN_LENGTH, {
    message: Error.NAME_MIN_LENGTH,
  })
  name: string;

  @IsString()
  @MinLength(DESCRIPTION_MIN_LENGTH, {
    message: Error.DESCRIPTION_MIN_LENGTH,
  })
  description: string;

  @IsNumberString()
  @IsCurrentPriceLessThanOrEqualBasePrice({
    message: Error.INVALID_CURRENT_PRICE,
  })
  currentPrice: number;

  @IsNumberString()
  basePrice: number;

  @IsValidCategoryId({ message: Error.CATEGORY_NOT_FOUND })
  categoryId: number;

  @IsString()
  image: string;
}