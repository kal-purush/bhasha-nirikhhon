import { PaymentStatus } from './../../constants/payment';
import { IsEnum, IsNumberString, IsOptional, IsString } from 'class-validator';
import { PageOptionsDto } from './pageOptions.dto';
import { 
  OrderSortField,
  OrderStatus,
  OrderType, 
} from '../../constants';
import { GreaterThanOrEqual } from '../../decorators';

export class OrderPageOptions extends PageOptionsDto {
  @IsEnum(OrderSortField, { message: 'error.enumNotMatch.orderSortField' })
  sortField: OrderSortField = OrderSortField.CREATED_AT;

  @IsOptional()
  @IsEnum(
    OrderType, 
    { message: 'error.enumNotMatch.orderType' },
  )
  orderType?: OrderType;

  @IsOptional()
  @IsEnum(
    OrderStatus, 
    { message: 'error.enumNotMatch.orderStatus' },
  )
  orderStatus?: OrderStatus;

  @IsOptional()
  @IsEnum(
    PaymentStatus, 
    { message: 'error.enumNotMatch.paymentStatus'},
  )
  paymentStatus?: PaymentStatus[];

  @IsOptional()
  @IsNumberString()
  userId?: number;

  @IsOptional()
  @GreaterThanOrEqual(0, { message: 'error.price.minValue' })
  minValue?: number;

  @IsOptional()
  @GreaterThanOrEqual(0, { message: 'error.price.minValue' })
  maxValue?: number;
}