import { Message, MessageType, User } from '@communecar/types';

import { graphqlRequest } from '../graphql';

type node = {
  status: string;
  userId: number;
};

type communityNode = node & {
  communityByCommunityId: {
    id: number;
    title: string;
  };
  userByUserId: User;
};

type rideNode = node & {
  rideId: number;
  userByUserId: User;
  rideByRideId: {
    toName?: string;
  };
};

type allRequestsQuery = {
  allUserRides: {
    nodes: rideNode[];
  };
  allUserCommunities: {
    nodes: communityNode[];
  };
};

const fetchUsersRequests = async (userId: number): Promise<Message[]> => {
  const allRequests = await graphqlRequest<allRequestsQuery>(
    allRequestsQuery(userId),
  );

  const communityMessages: Message[] = allRequests.allUserCommunities.nodes.map(
    (node) => {
      return {
        id: node.communityByCommunityId.id + node.status,
        time: new Date(Date.now()),
        type:
          node.status === 'Active'
            ? MessageType.APPROVED_COMMUNITY_REQUEST
            : node.status === 'Pending'
              ? MessageType.JOINING_COMMUNITY_REQUEST
              : MessageType.DECLINE_COMMUNITY_REQUEST,
        addresseeUsers: [node.userByUserId],
        creatorUser: node.userByUserId,
        entityId: node.communityByCommunityId.id,
        entityName: node.communityByCommunityId.title,
      };
    },
  );

  const rideMessages: Message[] = allRequests.allUserRides.nodes.map((node) => {
    return {
      id: node.rideId + node.status,
      time: new Date(Date.now()),
      type:
        node.status === 'Confirmed'
          ? MessageType.APPROVED_RIDE_REQUEST
          : node.status === 'Pending'
            ? MessageType.JOINING_RIDE_REQUEST
            : MessageType.DECLINE_RIDE_REQUEST,
      addresseeUsers: [node.userByUserId],
      creatorUser: node.userByUserId,
      entityId: node.rideId,
      entityName: node.rideByRideId.toName ?? 'no name specified',
    };
  });

  return [...communityMessages, ...rideMessages];
};

const allRequestsQuery = (userId: number): string => {
  return `{
        allUserRides(condition: {userId: ${userId}}){
          nodes {
            status
            userId
            rideId
            rideByRideId {
                toName
            }
            userByUserId {
                id
                firstName
                lastName
                email
                phone: phoneNumber
                gender
                age
            }
          }
        }
        allUserCommunities(condition: {userId: ${userId}}){
          nodes{
            status
            userId
            userByUserId{
                id
                firstName
                lastName
                email
                phone: phoneNumber
                gender
                age
            }
            communityByCommunityId{
                id
                title
              }
          }
        }
      }`;
};

export { fetchUsersRequests };