import gql from "graphql-tag";
import { useQuery, useLazyQuery } from "@apollo/react-hooks";

export interface GetUserQueryResult {
  user: {
    id: string;
    name: string;
    email: string;
  };
}

const GET_USER_QUERY = gql`
  query getUser {
    user {
      id
      name
      email
    }
  }
`;

export const useGetLoggedInUserQuery = () =>
  useQuery<GetUserQueryResult>(GET_USER_QUERY);

export const useLazyGetLoggedInUserQuery = () =>
  useLazyQuery<GetUserQueryResult>(GET_USER_QUERY);