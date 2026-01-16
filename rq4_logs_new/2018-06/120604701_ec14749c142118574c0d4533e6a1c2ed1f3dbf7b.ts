import { call, cancel, cancelled, fork, put, select, take, takeEvery } from 'redux-saga/effects';
import {
	SEARCH_FARE_FAMILIES_RT,
	SearchFareFamiliesRTAction
} from '../actions';
import FareFamiliesCombinations from '../../schemas/FareFamiliesCombinations';
import { clearSelectedFamilies, selectFamily } from '../fareFamilies/selectedFamilies/actions';
import loadFareFamilies from '../../services/requests/fareFamilies';
import { clearCombinations, setCombinations } from '../fareFamilies/fareFamiliesCombinations/actions';
import { startLoadingFareFamilies, stopLoadingFareFamilies } from '../isLoadingFareFamilies/actions';

function* worker({ payload }: SearchFareFamiliesRTAction) {
	const { flightId } = payload;

	try {
		yield put(startLoadingFareFamilies(0));
		yield put(clearCombinations());
		yield put(clearSelectedFamilies());

		// Get fare families combinations for given flight.
		const results: FareFamiliesCombinations = yield call(loadFareFamilies, flightId);

		// // Put them in Store.
		// yield put(setCombinations(legId, results));
		//
		// // Split initial combination key apart.
		// const combinationParts = results ? results.initialCombination.split('_') : [];
		// const numOfSegments = combinationParts.length;
		//
		// // Pre-select fare families.
		// for (let segmentId = 0; segmentId < numOfSegments; segmentId++) {
		// 	const familyId = combinationParts[segmentId];
		//
		// 	yield put(selectFamily(legId, segmentId, familyId));
		// }

		yield put(stopLoadingFareFamilies(0));
	}
	catch (error) {
		yield put(stopLoadingFareFamilies(0));
	}
}

export default function* searchFareFamiliesRTSaga() {
	yield takeEvery(SEARCH_FARE_FAMILIES_RT, worker);
}