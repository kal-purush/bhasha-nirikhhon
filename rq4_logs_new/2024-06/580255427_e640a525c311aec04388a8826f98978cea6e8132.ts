import { Leave } from './leave';
import { MultiProject } from './multi_project';
import { User } from './user';
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

export class Project {
  @ApiProperty({ type: Number })
  id: number;

  @ApiPropertyOptional({ type: Number })
  projectLeaderId?: number;

  @ApiPropertyOptional({ type: Number })
  projectSubLeaderId?: number;

  @ApiProperty({ type: String })
  name: string;

  @ApiPropertyOptional({ type: Date })
  createdAt?: Date;

  @ApiPropertyOptional({ type: Date })
  updatedAt?: Date;

  @ApiProperty({ isArray: true, type: () => Leave })
  leaves: Leave[];

  @ApiProperty({ isArray: true, type: () => MultiProject })
  multiProjects: MultiProject[];

  @ApiPropertyOptional({ type: () => User })
  projectLeader?: User;

  @ApiPropertyOptional({ type: () => User })
  projectSubLeader?: User;
}