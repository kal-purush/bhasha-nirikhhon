import { Question } from "@/stores/MyStudy/type";
import { useRecoilState } from "recoil";
import { useQuery, UseQueryResult } from "@tanstack/react-query";

import { quizQuestionState, currentQuizNumberState } from "@/modules/MyStudy/Quiz/StartQuiz/store";
import { DataTypes, QuestionTypes, TQuizTakeResponse } from "@/modules/MyStudy/Quiz/StartQuiz/type";
import { GetQuizById } from "@/modules/MyStudy/Quiz/StartQuiz/service";

export const useQuizQuestion = (): QuestionTypes => {
  const [getQuestion, setQuestion] = useRecoilState(quizQuestionState);
  return {
    setQuestionsData: (val: Array<Question>) => setQuestion(val),
    getQuestionsData: getQuestion,
  };
};

export const useCurrentQuizNumber = (): DataTypes => {
  const [getCurrentState, setCurrentData] = useRecoilState(currentQuizNumberState);
  return {
    setCurrNumber: (val: number) => setCurrentData(val),
    getCurrNumber: getCurrentState,
  };
};

// Quiz Service

export const useGetQuizById = (id: string): UseQueryResult<TQuizTakeResponse> =>
  useQuery<TQuizTakeResponse>({
    queryKey: ["get-quiz-by-id", id],
    queryFn: async () => await GetQuizById(id),
  });