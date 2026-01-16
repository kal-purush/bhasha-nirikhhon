import gql from "graphql-tag";
import { useMutation } from "@apollo/react-hooks";

export interface CreateBoardMutation {
  board: {
    id: number;
    title: string;
    backgroundColor: string;
    columnCount: number;
    taskCount: number;
  }[];
}

interface CreateBoardMutationVariables {
  title: string;
  backgroundColor: string;
}

export const CREATE_BOARD_MUTATION = gql`
  mutation createBoard($title: String!, $backgroundColor: String!) {
    createBoard(title: $title, backgroundColor: $backgroundColor) {
      id
      title
      backgroundColor
      columnCount
      taskCount
    }
  }
`;

export const useCreateBoardMutation = (onCompleted: () => void) =>
  useMutation<CreateBoardMutation, CreateBoardMutationVariables>(
    CREATE_BOARD_MUTATION,
    {
      update(cache) {
        //     cache.writeQuery({
        //       query: gql(listBoards),
        //       data: { boards: boards.concat([createBoard]) }
        //     });
        //     const cachedBoards = cache.readQuery({ query: gql(LIST_BOARD_PREVIEWS_QUERY) });
        //     const newState = Object.assign({}, prev, {
        //       boards: [...prev.boards, subscriptionData.data.board]
        //     });
        //     console.log(newState);
        //     return newState;
      },
      onCompleted() {
        onCompleted();
      }
    }
  );