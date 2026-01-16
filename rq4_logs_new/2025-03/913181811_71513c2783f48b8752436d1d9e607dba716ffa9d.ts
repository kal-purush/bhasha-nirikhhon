import { ApiProperty } from '@nestjs/swagger';
import { UserSubscription } from 'src/constants/user';

export class UserComment {
  @ApiProperty({ type: String })
  id: string;

  @ApiProperty({ type: String })
  email: string;

  @ApiProperty({ type: String })
  name: string;

  @ApiProperty({ type: String })
  given_name: string;

  @ApiProperty({ type: String })
  family_name: string;

  @ApiProperty({ type: String })
  picture: string | null;

  @ApiProperty({ enum: UserSubscription, default: UserSubscription.FREE })
  subscription: UserSubscription;

  @ApiProperty({ type: String })
  timestamp: string;
}