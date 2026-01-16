import {
  MutationSetEventQuestionsArgs,
  SetEventQuestionsInput
} from "@/apollo/types";
import { useMutation, useQuery } from "@vue/apollo-composable";
import gql from "graphql-tag";

export const createQuestion = () => {
  const { mutate: sendQuestions, onDone: onSendQuestionsDone } = useMutation<
    SetEventQuestionsInput,
    MutationSetEventQuestionsArgs
  >(gql`
    mutation setEventQuestions($input: SetEventQuestionsInput!) {
      setEventQuestions(input: $input)
    }
  `);
  return { sendQuestions, onSendQuestionsDone };
};