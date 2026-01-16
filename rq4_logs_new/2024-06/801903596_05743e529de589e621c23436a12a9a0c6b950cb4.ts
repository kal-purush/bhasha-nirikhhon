import { IsEnum, IsOptional } from 'class-validator';
import { Error, MIN_ADDRESS_LENGTH, OrderType, PaymentType } from '../../constants';
import { IsValidOrderAddress, IsValidOrderId } from '../../decorators';

export class CreateOrderDto {
  @IsEnum(OrderType, { message: Error.INVALID_ORDER_TYPE })
  orderType: OrderType;

  @IsEnum(PaymentType, { message: Error.INVALID_PAYMENT_TYPE })
  paymentType: PaymentType;

  @IsOptional()
  @IsValidOrderAddress(
    MIN_ADDRESS_LENGTH, 
    { message: Error.ADDRESS_MIN_LENGTH },
  )
  address?: string;

  @IsOptional()
  note?: string;

  @IsOptional()
  @IsValidOrderId({message: Error.STORE_NOT_FOUND})
  storeId?: number;
};