import { Action, Dispatch } from 'redux';
import _ from 'lodash';
import quizService from '../../services/quiz.service';
import { doFailure, doRequest, doSuccess } from './utils';

export const QuizActions = {
  GET_QUIZZES: '[Quiz] Get Quizzes',
  GET_QUIZZES_SUCCESS: '[Quiz] Get Quizzes Success',
  GET_QUIZZES_FAIL: '[Quiz] Get Quizzes Fail',

  GET_QUIZ: '[Quiz] Get Quiz',
  GET_QUIZ_SUCCESS: '[Quiz] Get Quiz Success',
  GET_QUIZ_FAIL: '[Quiz] Get Quiz Fail',

  CREATE_QUIZ: '[Quiz] Create Quiz',
  CREATE_QUIZ_SUCCESS: '[Quiz] Create Quiz Success',
  CREATE_QUIZ_FAIL: '[Quiz] Create Quiz Fail',

  GET_QUIZ_RESULT: '[Quiz] Get Quiz Result',
  GET_QUIZ_RESULT_SUCCESS: '[Quiz] Get Quiz Result Success',
  GET_QUIZ_RESULT_FAIL: '[Quiz] Get Quiz Result Fail',

  GET_QUESTIONS: '[Quiz] Get Questions',
  GET_QUESTIONS_SUCCESS: '[Quiz] Get Questions Success',
  GET_QUESTIONS_FAIL: '[Quiz] Get Questions Fail',
};

export interface GetQuizzesSuccessAction extends Action {
  payload: {
    quizzes: any[];
  };
}

export interface GetQuizzesFailAction extends Action {
  payload: {
    error: string;
  };
}

export interface GetQuestionsSuccessAction extends Action {
  payload: {
    questions: any[];
  };
}

export interface GetQuestionsFailAction extends Action {
  payload: {
    error: string;
  };
}

const getQuizzes = () => (dispatch: Dispatch): void => {
  dispatch(doRequest(QuizActions.GET_QUIZZES, {}));

  quizService.getQuizzes()
    .then((result: any) => dispatch(doSuccess(QuizActions.GET_QUIZZES_SUCCESS, { quizzes: result.data })))
    .catch((error) => dispatch(doFailure(QuizActions.GET_QUIZZES_FAIL, { error: _.get(error, ['response', 'data', 'message']) })));
};

const getQuestions = (quizId: string) => (dispatch: Dispatch) => {
  dispatch(doRequest(QuizActions.GET_QUESTIONS, {}));

  quizService.getQuestions(quizId)
    .then((result: any) => dispatch(doSuccess(QuizActions.GET_QUESTIONS_SUCCESS, { questions: result.data })))
    .catch((error) => dispatch(doFailure(QuizActions.GET_QUESTIONS_FAIL, { error: _.get(error, ['response', 'data', 'message']) })));
};

export default {
  getQuizzes,
  getQuestions,
};