import * as ko from 'knockout';
import { Loader as MapLoader } from '@googlemaps/js-api-loader';
import { ForeCastModel } from './forecastModel';
import { apiKey } from './utils/apiKey';

export class BindingContext {
    mapLoader: MapLoader;
    googleMap: google.maps.Map;
    forecast: ko.Observable<ForeCastModel> = ko.observable();
    constructor() {
        this.mapLoader = new MapLoader({
            apiKey: apiKey,
            version: 'weekly',
        });
        this.mapLoader.load().then(() => {
            this.googleMap = new google.maps.Map(document.getElementById('google-map') as HTMLElement, {
                center: { lat: 59.57, lng: 30.19 },
                zoom: 8,
            });
            this.forecast(new ForeCastModel(this.googleMap));
        });
    }
}

window.onload = () => {
    ko.applyBindings(new BindingContext(), document.getElementById('page-wrapper'));
};