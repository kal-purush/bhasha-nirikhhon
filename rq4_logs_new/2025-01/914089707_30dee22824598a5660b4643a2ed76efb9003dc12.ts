import {
  IsDate,
  IsInt,
  IsNotEmpty,
  IsOptional,
  IsString,
  ValidateNested,
} from 'class-validator';
import { AnimalEntity } from '../entities/animal.entity';
import { Type } from 'class-transformer';

export class CreateTripDto {
  @IsInt()
  @IsNotEmpty()
  readonly id: number;

  @IsString()
  @IsNotEmpty()
  readonly departure: string;

  @IsString()
  @IsNotEmpty()
  readonly arrival: string;

  @IsDate()
  @IsNotEmpty()
  readonly start_date: Date;

  @IsDate()
  @IsNotEmpty()
  readonly end_date: Date;

  @IsInt()
  @IsOptional()
  readonly complete_mission: number;

  @ValidateNested()
  @Type(() => AnimalEntity)
  @IsOptional()
  readonly animal_image: AnimalEntity;

  @ValidateNested()
  @Type(() => AnimalEntity)
  @IsOptional()
  readonly final_animal: AnimalEntity;
}