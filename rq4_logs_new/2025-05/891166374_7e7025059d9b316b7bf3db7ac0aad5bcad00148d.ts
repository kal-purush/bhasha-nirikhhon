import { IsNumber, IsOptional, Max, Min } from 'class-validator';
import { ApiProperty } from '@nestjs/swagger';

export class CardLocationDto {
  @ApiProperty({
    description: 'Latitud de la ubicación',
    example: 40.7128,
    minimum: -90,
    maximum: 90
  })
  @IsNumber()
  @Min(-90)
  @Max(90)
  latitude: number;

  @ApiProperty({
    description: 'Longitud de la ubicación',
    example: -74.0060,
    minimum: -180,
    maximum: 180
  })
  @IsNumber()
  @Min(-180)
  @Max(180)
  longitude: number;

  @ApiProperty({
    description: 'Precisión de la ubicación en metros',
    example: 10.5,
    required: false
  })
  @IsNumber()
  @IsOptional()
  accuracy?: number;
} 