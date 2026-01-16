import * as APIParser from '@nemo.travel/api-parser';
import AvailabilityInfo from '../../schemas/AvailabilityInfo';
import { parse as parseFlight } from './flight';

export const parse = (response: APIParser.Response, parentFlightId: string): AvailabilityInfo => {
	let result: AvailabilityInfo;
	const objects = APIParser(response);
	const responseInfo = objects[`flight/actualization/${parentFlightId}`];

	if (responseInfo) {
		result = {
			isAvailable: responseInfo['isAvailable'],
			orderLink: responseInfo['orderLink'],
			priceInfo: responseInfo['priceInfo'],
			flight: parseFlight(responseInfo['flight'])
		};
	}

	return result;
};