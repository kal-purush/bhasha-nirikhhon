import { Users } from './users';
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

export class ChangeScheduleRequests {
  @ApiProperty({ type: Number })
  id: number;

  @ApiProperty({ type: Number })
  userId: number;

  @ApiPropertyOptional({ type: Boolean })
  isManagerApproved?: boolean;

  @ApiPropertyOptional({ type: Boolean })
  isLeaderApproved?: boolean;

  @ApiProperty({ type: String })
  data: string;

  @ApiPropertyOptional({ type: Date })
  createdAt?: Date;

  @ApiPropertyOptional({ type: Date })
  updatedAt?: Date;

  @ApiProperty({ type: () => Users })
  user: Users;
}