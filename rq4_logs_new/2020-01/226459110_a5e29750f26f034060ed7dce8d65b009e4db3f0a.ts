import gql from "graphql-tag";
import { useMutation } from "@apollo/react-hooks";

export interface GoogleLoginMutation {
  authGoogle: {
    token: string;
    name: string;
    id: string;
  };
}

interface GoogleLoginMutationVariables {
  token: string;
}

export const GOOGLE_LOGIN_MUTATION = gql`
  mutation authGoogle($token: String) {
    authGoogle(token: $token) {
      token
      user {
        id
      }
    }
  }
`;

export const useGoogleLoginMutation = (onCompleted: () => void) =>
  useMutation<GoogleLoginMutation, GoogleLoginMutationVariables>(
    GOOGLE_LOGIN_MUTATION,
    {
      update(cache, mutationResult) {
        if (
          mutationResult &&
          mutationResult.data &&
          mutationResult.data.authGoogle &&
          mutationResult.data.authGoogle.token
        ) {
          localStorage.set(
            "torello_auth_token",
            mutationResult.data.authGoogle.token
          );
        }
      },
      onCompleted() {
        onCompleted();
      }
    }
  );