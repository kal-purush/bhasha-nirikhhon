import { all, takeEvery } from 'redux-saga/effects';

import {
  createHeroSaga,
  deleteHeroSaga,
  fetchHeroSaga,
  fetchHeroesSaga,
  updateHeroSaga
} from './hero/heroSaga';
import {
  CREATE_HERO,
  DELETE_HERO,
  FETCH_HERO,
  FETCH_HEROES,
  UPDATE_HERO
} from './hero/heroTypes';

/**
 * @name watchHero
 * @description hero saga combinator
 */
export function* watchHero() {
  yield all([
    takeEvery(CREATE_HERO, createHeroSaga),
    takeEvery(DELETE_HERO, deleteHeroSaga),
    takeEvery(FETCH_HERO, fetchHeroSaga),
    takeEvery(FETCH_HEROES, fetchHeroesSaga),
    takeEvery(UPDATE_HERO, updateHeroSaga)
  ]);
}