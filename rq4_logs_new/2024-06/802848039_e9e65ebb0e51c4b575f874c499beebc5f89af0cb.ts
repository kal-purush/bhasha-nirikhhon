import { Profile, SessionEntity, UserId } from '../_domain/types';
import { userRepository } from '../_repositories/user.repository';
import { createUserAbility } from '../_domain/user-ability';
import { AuthorizationError } from '@/shared/lib/errors';

type GetProfile = {
  userId: UserId;
  session: SessionEntity;
};

export class GetUserProfileUseCase {
  async exec({ userId, session }: GetProfile): Promise<Profile> {
    const userAbility = createUserAbility(session);
    if (!userAbility.canUpdateProfile(userId)) {
      throw new AuthorizationError();
    }
    return await userRepository.getUserById(userId);
  }
}

export const getUserProfileUseCase = new GetUserProfileUseCase();