import * as ko from 'knockout';
import { CityModel, citiesInfo } from '../../utils/_initData';
import { apiKey } from '../../utils/apiKey';
import { Loader as MapLoader } from '@googlemaps/js-api-loader';
import { IPageComponent } from '../pageComponent';

export class MapPageModel implements IPageComponent {
    constructor() {
        this.cities(citiesInfo);
        this.city(citiesInfo[0]);
        this.mapLoader = new MapLoader({
            apiKey: apiKey,
            version: 'weekly',
        });
        this.popupOptions = {
            width: 300,
            height: 280,
            container: '.page-viewport',
            contentTemplate: 'info',
            showTitle: true,
            title: () => this.city().name,
            visible: this.visiblePopup,
            dragEnabled: false,
            closeOnOutsideClick: true,
            showCloseButton: false,
            position: {
                at: 'top',
                my: 'center',
                of: this.positionOf,
            },
            toolbarItems: [{
                widget: 'dxButton',
                toolbar: 'bottom',
                location: 'after',
                options: {
                    text: 'Close',
                    onClick: () => this.visiblePopup(false)
                }
            }, {
                widget: 'dxButton',
                toolbar: 'bottom',
                location: 'before',
                options: {
                    text: 'Show on Map',
                    onClick: () => {
                        this.reloadComponent();
                        this.visiblePopup(false);
                    }
                }
            }]
        };
    }
    reloadComponent() {
        this.googleMap && this.googleMap.setCenter(this.city().coords);
    }
    showInfo(data) {
        this.city(data.model);
        this.positionOf(`#image${this.city().id}`);
        this.visiblePopup(true);
    }
    popupOptions: any;
    positionOf = ko.observable('');
    visiblePopup = ko.observable(false);
    googleMap: google.maps.Map;
    mapLoader: MapLoader;
    cities = ko.observableArray();
    city: ko.Observable<CityModel> = ko.observable();
    templateName = 'map-page-content';
}

ko.bindingHandlers['GoogleMap'] = {
    init: function(element: HTMLElement, valueAccessor, allBindings, viewModel, bindingContext) {
        var value: MapPageModel = ko.unwrap(valueAccessor() || {});
        value.mapLoader.load().then(() => {
            value.googleMap = new google.maps.Map(element, {
                center: value.city().coords,
                zoom: 8,
            });
        });
    }
};