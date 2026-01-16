import { Community } from '@communetypes/Community';
import { graphqlRequest } from '../graphql';

interface UserNode {
  profileImage: string | null;
}

interface UserByUserIdNode {
  userByUserId: UserNode;
}

interface CommunityByCommunityIdNode {
  id: number;
  title: string;
  description: string;
  userCommunitiesByCommunityId: {
    nodes: UserByUserIdNode[];
  };
}

interface UserCommunityNode {
  communityByCommunityId: CommunityByCommunityIdNode;
}

interface UserCommunitiesData {
  allUserCommunities: {
    nodes: UserCommunityNode[];
  };
}

const fetchAllUserCommunities = async (
  userId: number,
): Promise<Community[]> => {
  const query = `
  {
    allUserCommunities(condition: {userId: ${userId},  status: "Active"}) {
      nodes {
        communityByCommunityId {
          title
          id
          description
          userCommunitiesByCommunityId {
            nodes {
              userByUserId {
                profileImage
              }
            }
          }
        }
      }
    }
  }`;

  const data = await graphqlRequest<UserCommunitiesData>(query);

  const userCommunities: Community[] = [];

  data.allUserCommunities.nodes.forEach((node) => {
    const picturesUrl =
      node.communityByCommunityId.userCommunitiesByCommunityId.nodes
        .map((userByUserIdNode) => userByUserIdNode.userByUserId.profileImage)
        .filter((url): url is string => url != null);
    const community = {
      id: node.communityByCommunityId.id,
      title: node.communityByCommunityId.title,
      description: node.communityByCommunityId.description,
      numberOfMembers:
        node.communityByCommunityId.userCommunitiesByCommunityId.nodes.length,
      picturesUrl,
    };
    userCommunities.push(community);
  });
  return userCommunities;
};

export { fetchAllUserCommunities };