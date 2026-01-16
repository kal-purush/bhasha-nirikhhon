import { ApiProperty } from '@nestjs/swagger';
import { UserProfile } from '../entities/user-profile.entity';
import { Role } from 'src/auth/roles/role.enum';

export class ResponseUserProfileShortDto {
  @ApiProperty()
  name: string;

  @ApiProperty()
  surname: string;

  @ApiProperty({ type: 'number' })
  rating: number;

  @ApiProperty({ enum: Role })
  role: Role;

  static fromEntityPartial(userProfile: UserProfile): Omit<ResponseUserProfileShortDto, 'rating'> {
    return {
      name: userProfile.name,
      surname: userProfile.surname,
      role: userProfile.user.role,
    };
  }
}