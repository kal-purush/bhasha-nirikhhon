import { IsNotEmpty, IsString } from 'class-validator';

export class CreateCharacterDto {
  @IsString()
  @IsNotEmpty()
  name: string;

  @IsString()
  @IsNotEmpty()
  race: string;

  @IsString()
  @IsNotEmpty()
  subrace: string;

  @IsString()
  @IsNotEmpty()
  class: string;

  @IsString()
  @IsNotEmpty()
  level: string;

  @IsString()
  @IsNotEmpty()
  ability: string;

  @IsString()
  @IsNotEmpty()
  feat: string;

  @IsString()
  @IsNotEmpty()
  spell: string;

  @IsString()
  @IsNotEmpty()
  alignment: string;

  @IsString()
  @IsNotEmpty()
  skill: string;

  @IsString()
  @IsNotEmpty()
  equipment: string;
}