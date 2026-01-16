import gql from "graphql-tag";
import { useMutation } from "@apollo/react-hooks";

export interface CreateTaskMutation {
  task: {
    id: string;
    title: string;
    description: string;
    position: number;
  };
}

interface CreateTaskMutationVariables {
  columnId: string;
  title: string;
  description: string;
  position: number;
}

export const CREATE_TASK_MUTATION = gql`
  mutation createTask(
    $columnId: ID!
    $title: String!
    $description: String!
    $position: Int!
  ) {
    createTask(
      columnId: $columnId
      title: $title
      description: $description
      position: $position
    ) {
      id
      title
      description
      position
    }
  }
`;

export const useCreateTaskMutation = (onCompleted: () => void) =>
  useMutation<CreateTaskMutation, CreateTaskMutationVariables>(
    CREATE_TASK_MUTATION,
    {
      onCompleted() {
        onCompleted();
      }
    }
  );