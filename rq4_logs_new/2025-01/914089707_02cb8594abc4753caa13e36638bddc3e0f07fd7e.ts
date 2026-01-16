import { IsNotEmpty, IsNumber } from 'class-validator';
// import { MissionLevel } from '../entities/mission.entity';

export class CreateUserMissionDto {
  @IsNotEmpty()
  @IsNumber()
  readonly id: number;

  // @IsOptional()
  // @IsString()
  // readonly mission_name: string;

  // @IsOptional()
  // @IsString()
  // readonly level: MissionLevel;

  // @IsOptional()
  // thumbnail: string;

  // @IsOptional()
  // image: string;

  // @IsOptional()
  // @IsString()
  // description: string;
}