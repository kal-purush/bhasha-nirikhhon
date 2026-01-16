import * as constants from './constants';
import XHR from './utils/xhr';

const xhr = new XHR();

export default class Weather {
    public init(position: Position, count: number) {
        const coords = position.coords;

        constants.geoLocation.textContent = `${coords.latitude}, ${coords.longitude}`;

        xhr.GET(`${constants.GEO_URL}find?lat=${coords.latitude}&lon=${coords.longitude}
            &cnt=${count}&appid=${constants.GEO_API_KEY}`);
    }
}