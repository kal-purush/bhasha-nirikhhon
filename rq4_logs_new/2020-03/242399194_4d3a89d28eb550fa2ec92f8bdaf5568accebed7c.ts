import { Moment } from 'moment';

export interface FormState {
  title: string;
  description: string;
  date: Moment | null;
  startTime: Moment | null;
  endTime: Moment | null;
}
export type DateTimeFields = keyof Pick<
  FormState,
  'date' | 'startTime' | 'endTime'
>;

interface State {
  loading: boolean;
}

type Reducer = FormState & State;

interface StartRequest {
  type: 'START_REQUEST';
}
const START_REQUEST = 'START_REQUEST';
export const startRequest = (): StartRequest => ({
  type: START_REQUEST,
});

interface EndRequest {
  type: 'END_REQUEST';
}
const END_REQUEST = 'END_REQUEST';
export const endRequest = (): EndRequest => ({
  type: END_REQUEST,
});

interface FormAction {
  type: 'FORM_ACTION';
  payload: {
    field: keyof FormState;
    value: string;
  };
}
const FORM_ACTION = 'FORM_ACTION';
export const formAction = (
  field: keyof FormState,
  value: string
): FormAction => ({
  type: FORM_ACTION,
  payload: { field, value },
});

interface DateTimeAction {
  type: 'DATE_TIME_ACTION';
  payload: {
    field: DateTimeFields;
    value: Moment | null;
  };
}
const DATE_TIME_ACTION = 'DATE_TIME_ACTION';
export const dateTimeAction = (
  field: DateTimeFields,
  value: Moment | null
): DateTimeAction => ({
  type: DATE_TIME_ACTION,
  payload: { field, value },
});

type Action = StartRequest | EndRequest | FormAction | DateTimeAction;

export const getInitialState = (dateTime: Moment): Reducer => ({
  title: '',
  description: '',
  date: dateTime,
  startTime: dateTime,
  endTime: dateTime.add(1, 'hours'),
  loading: false,
});

export const reducer = (state: Reducer, action: Action): Reducer => {
  switch (action.type) {
    case START_REQUEST: {
      return { ...state, loading: true };
    }
    case END_REQUEST: {
      return { ...state, loading: false };
    }
    case FORM_ACTION: {
      return { ...state, [action.payload.field]: action.payload.value };
    }
    case DATE_TIME_ACTION: {
      let { startTime, endTime } = state;

      if (action.payload.value) {
        if (
          action.payload.field === 'startTime' &&
          action.payload.value.isAfter(endTime || undefined)
        ) {
          endTime = action.payload.value.add(1, 'hours');
        }
        if (
          action.payload.field === 'endTime' &&
          action.payload.value.isBefore(startTime || undefined)
        ) {
          startTime = action.payload.value.subtract(1, 'hours');
        }
      }

      return {
        ...state,
        endTime,
        startTime,
        [action.payload.field]: action.payload.value,
      };
    }
    default:
      return state;
  }
};