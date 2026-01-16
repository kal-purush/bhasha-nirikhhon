import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { IsNotEmpty, IsUUID } from 'class-validator';
import * as jwt from 'jsonwebtoken';
import { NSMember } from '@libs/common/enums';

export class RegisterMemberReq {
  @ApiPropertyOptional({ description: 'Enter referral code' })
  referralCode?: string;

  @ApiPropertyOptional({ description: 'Username' })
  username?: string;

  @ApiPropertyOptional({ description: 'Password' })
  password: string;

  @ApiPropertyOptional({ nullable: true })
  addressType?: NSMember.EAddressType;

  @ApiProperty({
    enum: NSMember.EMembershipType,
    description: 'Membership type.',
    default: NSMember.EMembershipType.Personal,
  })
  membershipType: NSMember.EMembershipType;

  @ApiPropertyOptional({ description: 'Personal ID' })
  personalId?: string;

  @ApiPropertyOptional({ description: 'Tax code for business customers' })
  taxCode?: string;

  /** Address */
  @ApiProperty()
  // @IsNotEmpty()
  address: string;

  /** Phone number */
  @ApiProperty()
  @IsNotEmpty()
  phone: string;

  /** Email */
  @ApiPropertyOptional()
  email?: string;

  /** Full name */
  @ApiPropertyOptional()
  fullName?: string;
}

export class LoginReq {
  @ApiProperty()
  @IsNotEmpty()
  username: string;

  @ApiProperty()
  @IsNotEmpty()
  password: string;
}

export class JwtPayload implements jwt.JwtPayload {
  @ApiPropertyOptional()
  iss?: string | undefined;
  @ApiPropertyOptional()
  sub?: string | undefined;
  @ApiPropertyOptional()
  aud?: string | string[] | undefined;
  @ApiPropertyOptional()
  exp?: number | undefined;
  @ApiPropertyOptional()
  nbf?: number | undefined;
  @ApiPropertyOptional()
  iat?: number | undefined;
  @ApiPropertyOptional()
  jti?: string | undefined;
}

export class MemberSessionPayload extends JwtPayload {
  @ApiProperty()
  id: string;

  @ApiPropertyOptional()
  storeId?: string;

  /** Username */
  @ApiProperty()
  username: string;

  /** Password */
  @ApiProperty()
  password: string;

  @ApiProperty({ nullable: true })
  addressType: NSMember.EAddressType;

  @ApiProperty()
  personalId: string;

  @ApiProperty()
  phone: string;

  @ApiProperty({ enum: NSMember.EStatus })
  status: NSMember.EStatus;

  /** Email */
  @ApiPropertyOptional()
  email?: string;

  @ApiPropertyOptional()
  fullName?: string;

  @ApiPropertyOptional()
  address?: string;
}

export class MemberSessionDto extends MemberSessionPayload {
  @ApiProperty()
  accessToken: string;
  @ApiProperty()
  refreshToken: string;
  @ApiProperty()
  tokenType: 'Bearer' = 'Bearer';
}

export class ForgotPasswordReq {
  @ApiProperty()
  username: string;
}

export class VerifyOtpReq{
  @ApiProperty({ description: 'OTP Code' })
  otpCode: string;

  @ApiProperty({ description: 'User ID' })
  userId: string;
}

export class VerifyRegisterMemberReq {
  @ApiPropertyOptional({ description: 'Enter referral code' })
  referralCode?: string;

  @ApiPropertyOptional({ description: 'Username' })
  username?: string;
}