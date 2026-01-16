/* eslint-disable @typescript-eslint/explicit-function-return-type */
import { call, takeEvery, put } from 'redux-saga/effects';

import { loadFilters } from '../api/api';
import { loadFiltersSucceeded, loadFiltersFailed, IFilterDescription } from '../store/filter';
import { IAction } from '../store';
import { LOAD_FILTERS_REQUESTED } from '../store/filter/types';

function* loadFiltersWorker(action: IAction<string[]>) {
  try {
    const response: IFilterDescription[] = yield call(loadFilters, action.payload);
    yield put(loadFiltersSucceeded(response));
  } catch (e) {
    yield put(loadFiltersFailed());
  }
}

export default function* loadFiltersSaga() {
  yield takeEvery(LOAD_FILTERS_REQUESTED, loadFiltersWorker);
}