import { combineEpics } from 'redux-observable';
import { fetchUserEpic } from './user';

export const rootEpic = combineEpics(
    fetchUserEpic
);