import { AxiosError } from 'axios';
import { put } from 'redux-saga/effects';

import { actions } from '@client/store/reducers';

import * as errors from '@shared/errors';

export function* handleError(
    e?: AxiosError,
): Generator<unknown, void, unknown> {
    if (e && e.response) {
        yield put(actions.nav.setTemplateError(e.response.data.message));
    } else if (e) {
        yield put(actions.nav.setTemplateError(errors.NO_INTERNET));
    } else {
        yield put(actions.nav.setTemplateError(errors.INTERNAL));
    }
}