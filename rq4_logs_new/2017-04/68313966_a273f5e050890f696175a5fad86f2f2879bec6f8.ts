import { Action, ActionReducer } from '@ngrx/store';
import { Question } from '../Models/question';
import { PeerAssessmentActions } from '../Actions/peer-assessment.actions';
import { UserActions } from '../Actions/user.actions';

const INITIAL_STATE = [];

export const questionsReducer: ActionReducer<Question[]> = (state: Question[] = INITIAL_STATE, action: Action): Question[] => {
  switch (action.type) {

    case PeerAssessmentActions.GET_QUESTIONS_SUCCESS: {
      return [...action.payload];
    }

    case UserActions.USER_LOGOUT_SUCCESS: {
      return [];
    }

    default: return state;
  }
};