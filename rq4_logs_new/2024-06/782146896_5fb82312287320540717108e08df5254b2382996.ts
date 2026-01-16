import { UsersSelectorOption } from '../../types/users-selector-option';
import { graphqlRequest } from '../graphql';

const GET_ALL_USERS_QUERY = `
query {
  allUsers {
    nodes {
      email
      firstName
      lastName
      id
      phoneNumber
      profileImage
    }
  }
}
`;

interface GetAllUsersResponse {
  allUsers: {
    nodes: UserNode[];
  };
}

interface UserNode {
  id: number;
  firstName: string;
  lastName: string;
  email: string;
  phoneNumber: string;
  profileImage: string;
}

export const fetchAllUsers = async (): Promise<UsersSelectorOption[]> => {
  try {
    const data = await graphqlRequest<GetAllUsersResponse>(GET_ALL_USERS_QUERY);
    return data.allUsers.nodes.map((user) => ({
      label: `${user.firstName} ${user.lastName}`,
      userId: user.id,
      email: user.email,
      phone: user.phoneNumber,
      avatarUrl: user.profileImage,
    }));
  } catch (error) {
    console.error('Error fetching users:', error);
    throw error;
  }
};