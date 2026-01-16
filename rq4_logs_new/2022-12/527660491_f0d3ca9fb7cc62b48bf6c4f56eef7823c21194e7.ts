import { ApiProperty } from '@nestjs/swagger';
import {
  checkoutApiInformation,
  checkoutApiMessage,
} from '../../utils/api-messages';
import { UserAddressProp } from '../../user/props/user.props';
import { BasketFilterResponseProp } from '../../basket/props/basket.props';

export class CheckoutPlaceOrderResponseProp {
  @ApiProperty({
    type: Boolean,
    example: true,
  })
  isSuccess: boolean;

  @ApiProperty({
    type: String,
    example: checkoutApiMessage.placeOrderConfirmation,
  })
  message: string;
}

export class CheckoutOrderHistoryResponseProp {
  @ApiProperty({
    type: String,
    format: 'uuid',
    example: checkoutApiInformation.orderId,
  })
  id: string;

  @ApiProperty({
    type: Number,
    example: checkoutApiInformation.totalPrice,
  })
  total: number;

  @ApiProperty({
    type: UserAddressProp,
  })
  address: UserAddressProp;

  @ApiProperty({
    type: BasketFilterResponseProp,
    isArray: true,
  })
  items: BasketFilterResponseProp[];
}

export class CheckoutTotalPriceResponseProp {
  @ApiProperty({
    type: Number,
    example: checkoutApiInformation.totalPrice,
  })
  totalPrice: number;

  @ApiProperty({
    type: Number,
    example: checkoutApiInformation.totalItems,
  })
  totalItems: number;

  @ApiProperty({
    type: Number,
    example: checkoutApiInformation.itemsType,
  })
  itemsType: number;
}