import { gql } from '@apollo/client';

export const GET_HOUSEHOLDS = gql`
  query {
    households {
      _id
      name
    }
  }
`;

export const JOIN_HOUSEHOLD = gql`
  mutation JoinHousehold($userId: ID!, $householdId: ID!) {
    joinHousehold(userId: $userId, householdId: $householdId) {
      _id
      username
      household {
        _id
        name
      }
    }
  }
`;