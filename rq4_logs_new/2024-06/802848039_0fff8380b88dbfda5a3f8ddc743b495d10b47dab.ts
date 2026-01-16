import { Profile, SessionEntity, UserId } from '../_domain/types';
import { profileRepository } from '../_repositories/profile';
import { createProfileAbility } from '../_domain/ability';
import { AuthorizationError } from '@/shared/lib/errors';

type UpdateProfile = {
  userId: UserId;
  data: Partial<Profile>;
  session: SessionEntity;
};

export class UpdateProfileUseCase {
  async exec({ userId, session, data }: UpdateProfile): Promise<Profile> {
    const profileAbility = createProfileAbility(session);

    if (!profileAbility.canUpdateProfile(userId)) {
      throw new AuthorizationError();
    }
    return await profileRepository.update(userId, data);
  }
}

export const updateProfileUseCase = new UpdateProfileUseCase();