import { MultipleAnswerQuestion } from '@/types/Quiz'
import { createMultipleChoiceQuestion } from './newTestamentQuestionCreator'
import QuestionComponent from '@/pages/Challenges/components/Question.vue'
import BibleQuizQuestion from '@/pages/Challenges/questions/BibleQuizQuestion.vue'
import { random } from '@/helpers'

export enum QuestionTypes {
  'GuessTheReference',
  'GuessTheVerse',
  'Quiz',
}

export type QuestionComponentType =
  | typeof QuestionComponent
  | typeof BibleQuizQuestion

type QuestionObjectType = {
  generate: () => Promise<MultipleAnswerQuestion>
  component: QuestionComponentType
}

type QuestionsObjectType = {
  [key: number]: QuestionObjectType
}
export type QuestionType = {
  question: MultipleAnswerQuestion
  type: QuestionTypes
}
export type QuestionCreatorType = () => Promise<QuestionType>

export const Questions: QuestionsObjectType = {
  [QuestionTypes.GuessTheReference]: {
    generate: createMultipleChoiceQuestion,
    component: QuestionComponent,
  },
  [QuestionTypes.GuessTheVerse]: {
    generate: createMultipleChoiceQuestion,
    component: QuestionComponent,
  },
  [QuestionTypes.Quiz]: {
    generate: createMultipleChoiceQuestion,
    component: BibleQuizQuestion,
  },
}

export const createQuestionCreator = (
  ...types: QuestionTypes[]
): QuestionCreatorType => async () => {
  const randomType = types[random(1, types.length) - 1]
  const selectedQuestion = Questions[randomType]
  console.log(randomType)
  return {
    question: await selectedQuestion.generate(),
    type: randomType,
  }
}