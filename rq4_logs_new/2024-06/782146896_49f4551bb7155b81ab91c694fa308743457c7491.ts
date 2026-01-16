import { getUsersForCommunityId } from '../utils/communitiesQueries';
import { graphqlRequest } from '../graphql';
import { UsersSelectorOption } from '../../types/users-selector-option';
import { UserStatus } from '@communetypes/Enums';

interface GetAllUsersResponse {
  allUserCommunities: {
    nodes: { userByUserId: UserNode }[];
  };
}

interface UserNode {
  id: number;
  firstName: string;
  lastName: string;
  email: string;
  phoneNumber: string;
  profileImage: string;
  userCommunitiesByUserId: {
    nodes: userCommunitiesNode[];
  };
}

interface userCommunitiesNode {
  communityId: number;
  status: UserStatus;
}

const fetchRidersByCommunityId = async (
  communityId: number,
): Promise<UsersSelectorOption[]> => {
  const query = getUsersForCommunityId(communityId);

  const data = await graphqlRequest<GetAllUsersResponse>(query);

  return data.allUserCommunities?.nodes.map((user) => {
    const communitiesStatus =
      user.userByUserId.userCommunitiesByUserId?.nodes ?? [];
    return {
      label: `${user.userByUserId.firstName} ${user.userByUserId.lastName}`,
      userId: user.userByUserId.id,
      email: user.userByUserId.email,
      phone: user.userByUserId.phoneNumber,
      avatarUrl: user.userByUserId.profileImage,
      communitiesStatus,
    };
  });
};

export { fetchRidersByCommunityId };