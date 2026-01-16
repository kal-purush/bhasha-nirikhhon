import gql from "graphql-tag";
import { useMutation } from "@apollo/react-hooks";

export interface CreateColumnMutation {
  column: {
    id: string;
    title: string;
    position: number;
  };
}

interface CreateColumnMutationVariables {
  boardId: string;
  title: string;
  position: number;
}

export const CREATE_COLUMN_MUTATION = gql`
  mutation createColumn($boardId: ID!, $title: String!, $position: Int!) {
    createColumn(boardId: $boardId, title: $title, position: $position) {
      id
      title
      position
      tasks {
        id
        title
        description
      }
    }
  }
`;

export const useCreateColumnMutation = (onCompleted: () => void) =>
  useMutation<CreateColumnMutation, CreateColumnMutationVariables>(
    CREATE_COLUMN_MUTATION,
    {
      onCompleted() {
        onCompleted();
      }
    }
  );