import { RootState } from '../reducers';
import { FlightsByLegsState } from './reducers';
import { createSelector } from 'reselect';

/**
 * Get flights objects grouped by leg id.
 *
 * @param {RootState} state
 * @returns {FlightsByLegsState}
 */
export const getFlightsIdsByLegs = (state: RootState): FlightsByLegsState => state.flightsByLegs;

/**
 * Check if we have enough flights on each leg.
 * If we don't, then we have to show global "No results" error.
 */
export const hasAnyFlights = createSelector(
	[getFlightsIdsByLegs],
	(flightsByLegs: FlightsByLegsState): boolean => {
		let result = !!Object.keys(flightsByLegs).length;

		for (const legId in flightsByLegs) {
			if (flightsByLegs.hasOwnProperty(legId) && !flightsByLegs[legId].length) {
				result = false;
				break;
			}
		}

		return result;
	}
);