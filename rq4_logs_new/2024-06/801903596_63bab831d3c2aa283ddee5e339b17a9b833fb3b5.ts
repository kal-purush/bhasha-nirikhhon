import {
  IsDateString,
  IsEnum,
  IsNumberString,
  IsString,
  MinLength,
} from 'class-validator';
import {
  Error,
  Gender,
  MIN_ADDRESS_LENGTH,
  REGISTER_FIELD_MIN_LENGTH,
} from '../../constants';
import { IsValidDOB } from '../../decorators';

export class UpdatePersonalInfo {
  @IsString()
  @MinLength(REGISTER_FIELD_MIN_LENGTH, { message: 'error.fullName.minLength' })
  fullName: string;

  @IsEnum(Gender, { message: 'error.enumNotMatch.gender' })
  gender: Gender;

  @IsString()
  @MinLength(MIN_ADDRESS_LENGTH, { message: 'error.fullName.minLength' })
  address: string;

  @IsNumberString()
  @MinLength(10, { message: Error.PHONE_NUMBER_MIN_LENGTH })
  phoneNumber: string;

  @IsDateString()
  @IsValidDOB({ message: Error.INVALID_DOB })
  dob: Date;
}