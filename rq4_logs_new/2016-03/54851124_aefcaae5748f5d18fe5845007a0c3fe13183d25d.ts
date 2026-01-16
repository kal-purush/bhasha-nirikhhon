import {Component, View, CORE_DIRECTIVES} from 'angular2/angular2';
import {RouteParams} from 'angular2/router';

import {RdWidget} from '../rd-widget/rd-widget';
import {RdWidgetHeader} from '../rd-widget-header/rd-widget-header';
import {RdWidgetBody} from '../rd-widget-body/rd-widget-body';

import {CollectionListView} from 'components/collection-list-view/collection-list-view';
import {CountryListService} from '../../services/country_list';

@Component({
    selector: 'countries',
    bindings: [CountryListService]
})
@View({
    templateUrl: './components/countries/countries.html',
    directives: [CORE_DIRECTIVES, RdWidget, RdWidgetHeader, RdWidgetBody, CollectionListView]
})
export class Countries {
    modelItems: any[];

    constructor(private _countryService: CountryListService) {
        this.modelItems = _countryService.countries;
    }
}