import { IsNotEmpty, IsNumber, IsUUID } from 'class-validator';

export class CreateIncomeBody {
  @IsUUID()
  @IsNotEmpty({
    message: 'The category_uuid should not be empty.',
  })
  category_uuid: string;

  @IsNotEmpty({
    message: 'The description should not be empty.',
  })
  description: string;

  @IsNotEmpty({
    message: 'The date should not be empty.',
  })
  date: string;

  @IsNumber()
  @IsNotEmpty({
    message: 'The value should not be empty.',
  })
  value: number;

  @IsNotEmpty({
    message: 'The isReceived should not be empty.',
  })
  isReceived: boolean;
}