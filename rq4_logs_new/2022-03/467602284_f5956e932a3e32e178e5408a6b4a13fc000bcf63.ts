import { Optional } from '@nestjs/common';
import { ApiProperty, OmitType, PickType } from '@nestjs/swagger';
import { Exclude, Expose } from 'class-transformer';
import { IsEmail, isEmail, IsString, IsUUID } from 'class-validator';
import { AccountEntity } from '../model/account.entity';

@Exclude()
export class AccountProfileAddressDTO {
    @Expose()
    @ApiProperty({ required: false })
    id: string;

    @Expose()
    @ApiProperty({ required: true })
    @IsString()
    countryId: string;

    @Expose()
    @ApiProperty({ required: true })
    @IsString()
    countryName: string

    @Expose()
    @ApiProperty({ required: true })
    @IsString()
    stateId: string;

    @Expose()
    @ApiProperty({ required: true })
    @IsString()
    stateName: string

    @Expose()
    @ApiProperty({ required: true })
    @IsString()
    cityId: string;

    @Expose()
    @ApiProperty({ required: true })
    @IsString()
    cityName: string

    @Expose()
    @ApiProperty({ required: true })
    @IsString()
    addressDetails: string
}

@Exclude()
export class AccountProfileDTO {
    @Expose()
    @ApiProperty({ required: false })
    @IsUUID()
    id: string;

    @Expose()
    @ApiProperty({ required: true })
    @IsString()
    profileName: string;

    @Expose()
    @ApiProperty({ required: true })
    @IsString()
    firstName: string;

    @Expose()
    @ApiProperty({ required: true })
    @IsString()
    lastName: string;

    @Expose()
    @ApiProperty({ required: false })
    @IsString()
    contactNumber: string;

    @Expose()
    @ApiProperty({ required: false })
    createDateTime: Date;

    @ApiProperty({ required: true })
    @IsString()
    lastChangedBy: string;

    // @ApiProperty({ required: false })
    // @IsString()
    // address: AccountProfileAddressDTO;


}


export class createAccountProfileDTO extends PickType(AccountProfileDTO, ['profileName', 'firstName', 'lastName', 'contactNumber'] as const) { };
export class createAccountProfileAddressDTO extends PickType(AccountProfileAddressDTO, ['countryId', 'countryName', 'stateName', 'stateId', 'cityId', 'cityName', 'addressDetails'] as const) { };

