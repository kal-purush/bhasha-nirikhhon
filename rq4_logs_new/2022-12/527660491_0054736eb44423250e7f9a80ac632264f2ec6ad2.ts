import { ApiProperty } from '@nestjs/swagger';
import {
  basketApiInformation,
  productApiInformation,
} from '../../utils/api-messages';
import { ProductFilterResponseProp } from '../../product/props/product.props';
import { RemoveProductFromBasket } from '../../types';

export class AddToBasketResponseProp {
  @ApiProperty()
  isSuccess: true;

  @ApiProperty({
    type: String,
    format: 'uuid',
    example: productApiInformation.productId,
  })
  id: string;
}

export class BasketUpdateResponseProp {
  @ApiProperty()
  isSuccess: true;
}

export class BasketFilterResponseProp {
  @ApiProperty({
    type: String,
    format: 'uuid',
    example: basketApiInformation.basketId,
  })
  id: string;

  @ApiProperty({
    type: Number,
    minimum: 1,
    maximum: 5000,
    example: basketApiInformation.itemQuantity,
  })
  quantity: number;

  @ApiProperty({
    type: ProductFilterResponseProp,
  })
  product: ProductFilterResponseProp;
}

export class RemoveProductFromBasketProp implements RemoveProductFromBasket {
  @ApiProperty({
    type: Boolean,
    example: true,
  })
  isSuccess: boolean;
}