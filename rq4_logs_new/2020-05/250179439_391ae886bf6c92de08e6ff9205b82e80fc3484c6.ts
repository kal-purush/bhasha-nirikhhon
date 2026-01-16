import { Reducer } from 'redux';
import Topic from '../../models/Topic';
import { LessonAction } from '../actions/lesson';
import { ASSIGNMENT_SET_DATA } from '../actions/types';

export interface AssignmentState {
  loading: boolean;
  data: Array<Topic>;
  error: string;
}

const initState = {
  loading: false,
  data: [],
  error: '',
};

const assignmentState: Reducer<AssignmentState> = (
  state: AssignmentState = initState,
  action: LessonAction | any
): AssignmentState => {
  switch (action.type) {
    case ASSIGNMENT_SET_DATA: {
      return {
        ...state,
        data: action.assignment,
      };
    }
    default:
      return state;
  }
};

export default assignmentState;