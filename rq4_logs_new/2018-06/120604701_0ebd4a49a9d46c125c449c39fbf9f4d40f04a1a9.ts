import { call, put, select, takeEvery } from 'redux-saga/effects';
import { ActualizationAction, START_ACTUALIZATION } from '../../actions';
import { getIsLoadingActualization } from '../../isLoadingActualization/selectors';
import { startLoadingActualization, stopLoadingActualization } from '../../isLoadingActualization/actions';

function* runActualization(flightIds: string[]) {
	console.log(flightIds);

	yield true;
}

function* worker({ payload }: ActualizationAction) {
	const isLoading: boolean = yield select(getIsLoadingActualization);

	if (!isLoading) {
		yield put(startLoadingActualization());

		yield call(runActualization, payload.flightIds);

		yield put(stopLoadingActualization());
	}
}

export default function* actualizeFlightSaga() {
	yield takeEvery(START_ACTUALIZATION, worker);
}