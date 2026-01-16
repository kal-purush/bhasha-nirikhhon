import gql from "graphql-tag";
import { useQuery } from "@apollo/react-hooks";

export interface GetBoardQueryVariables {
  id: string;
}

export interface GetBoardQueryResult {
  board: {
    id: string;
    title: string;
    backgroundColor: string;
    columns: {
      id: string;
      title: string;
      position: number;
      tasks: {
        id: string;
        title: string;
        description: string;
        position: number;
      }[];
    }[];
  };
}

const GET_BOARD_QUERY = gql`
  query getBoard($id: ID!) {
    board(id: $id) {
      id
      title
      backgroundColor
      columns {
        id
        title
        position
        tasks {
          id
          title
          description
          position
        }
      }
    }
  }
`;

export const useGetBoardQuery = (id: string) =>
  useQuery<GetBoardQueryResult, GetBoardQueryVariables>(GET_BOARD_QUERY, {
    variables: {
      id
    }
  });