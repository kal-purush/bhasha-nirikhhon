import {
  LoginSuccessfulResponse,
  UserAddressInterface,
  UserInfoSuccessfulResponse,
  UserRole,
} from '../../types';
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

export class UserActivationProp {
  @ApiProperty()
  isSuccess: boolean;

  @ApiProperty()
  message: string;
}

export class UserAddressProp implements UserAddressInterface {
  @ApiProperty()
  id: string;

  @ApiProperty()
  address: string;

  @ApiProperty()
  city: string;

  @ApiProperty()
  postalCode: string;

  @ApiProperty()
  country: string;

  @ApiProperty()
  mobilePhone: number;
}

export class UserInfoResponseProp implements UserInfoSuccessfulResponse {
  @ApiProperty()
  id: string;

  @ApiProperty()
  firstName: string;

  @ApiProperty()
  lastName: string;

  @ApiProperty()
  email: string;

  @ApiProperty({
    type: UserAddressProp,
    isArray: true,
  })
  address: UserAddressProp[];

  @ApiProperty({
    enum: UserRole,
    enumName: 'UserRole',
    default: UserRole.USER,
  })
  role: UserRole;
}

export class LoginSuccessfulResponseProp implements LoginSuccessfulResponse {
  @ApiProperty()
  isSuccess: true;

  @ApiProperty()
  id: string;

  @ApiProperty()
  firstName: string;

  @ApiProperty()
  lastName: string;

  @ApiProperty({
    enum: UserRole,
    enumName: 'UserRole',
    default: UserRole.USER,
  })
  role: UserRole;
}

export class UserDeleteAccountProp {
  @ApiProperty()
  isSuccess: boolean;

  @ApiPropertyOptional()
  message: string;
}