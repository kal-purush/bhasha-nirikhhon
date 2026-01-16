/* eslint-disable prettier/prettier */
import { ApiProperty } from '@nestjs/swagger';

export class CreateInitiativeDto {
    @ApiProperty({
        description: 'The name of the initiative',
        required: true,
    })
    name: string;

    @ApiProperty({
        description: 'The description of the initiative',
        required: true,
    })
    description: string;

    @ApiProperty({
        description: 'The images of the initiative',
        required: true,
    })
    images: string[];
}