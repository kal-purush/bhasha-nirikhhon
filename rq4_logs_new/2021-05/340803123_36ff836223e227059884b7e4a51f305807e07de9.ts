import { CheckInMutation, MutationCheckInArgs } from "@/apollo/types";
import { useMutation } from "@vue/apollo-composable";
import gql from "graphql-tag";

export const checkInOrg = () => {
  const {
    mutate: checkIn,
    onDone: onCheckInDone,
    onError: onCheckInError
  } = useMutation<CheckInMutation, MutationCheckInArgs>(gql`
    mutation checkIn($input: CheckInInput!) {
      checkIn(input: $input) {
        id
        status
        user {
          id
          firstName
          lastName
        }
      }
    }
  `);
  return { checkIn, onCheckInDone, onCheckInError };
};