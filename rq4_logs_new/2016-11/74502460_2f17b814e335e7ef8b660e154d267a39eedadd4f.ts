import * as constants from './constants';
import XHR from './utils/xhr';

const xhr = new XHR();

export default class Geo {
    constructor(public count: number = 10) {
        if (!navigator.geolocation) {
            alert('Geolocation is not supported by your browser');
            return;
        }
    }

    public getPos(position: Position) {
        const coords = position.coords,
            elLocation = document.getElementById('geolocation');

        elLocation.textContent = `${coords.latitude}, ${coords.longitude}`;

        xhr.GET(`${constants.GEO_URL}find?lat=${coords.latitude}&lon=${coords.longitude}&cnt=${this.count}&appid=${constants.GEO_API_KEY}`);
    }

    private getPosError(): void {
        console.error('Unable to retrieve your location');
    }

    public getPosition(): void {
        navigator.geolocation.getCurrentPosition(this.getPos.bind(this), this.getPosError);
    }
}