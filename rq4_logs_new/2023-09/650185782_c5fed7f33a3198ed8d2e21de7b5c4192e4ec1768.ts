/* eslint-disable prettier/prettier */
import { ApiProperty } from '@nestjs/swagger';

export class UpdateInitiativeDto {
    @ApiProperty({
        description: 'The name of the initiative',
        required: false,
        nullable: true,
    })
    name: string;

    @ApiProperty({
        description: 'The description of the initiative',
        required: false,
        nullable: true,
    })
    description: string;

    @ApiProperty({
        description: 'The images of the initiative',
        required: false,
        nullable: true,
    })
    images: string[];

    @ApiProperty({
        description: 'The socials of the initiative',
        required: false,
        nullable: true,
    })
    socials: string[];
}