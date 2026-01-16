import {
  IsNumberString,
  IsString,
  MinLength,
  ValidationArguments,
} from 'class-validator';
import {
  Error,
  MIN_ADDRESS_LENGTH,
  STORE_PHONE_MIN_LENGTH,
} from '../../constants';
import { STORE_NAME_MIN_LENGTH } from '../../constants';
import { t } from 'i18next';

export class CreateStoreDto {
  @IsString()
  @MinLength(STORE_NAME_MIN_LENGTH, {
    message: (args: ValidationArguments) =>
      t(Error.STORE_NAME_MIN_LENGTH, { length: STORE_NAME_MIN_LENGTH }),
  })
  name: string;

  @IsString()
  @MinLength(MIN_ADDRESS_LENGTH, {
    message: (args: ValidationArguments) =>
      t(Error.ADDRESS_MIN_LENGTH, { length: MIN_ADDRESS_LENGTH }),
  })
  address: string;

  @IsNumberString()
  @MinLength(STORE_PHONE_MIN_LENGTH, {
    message: (args: ValidationArguments) =>
      t(Error.PHONE_NUMBER_MIN_LENGTH, { length: 10 }),
  })
  phoneNumber: string;
}