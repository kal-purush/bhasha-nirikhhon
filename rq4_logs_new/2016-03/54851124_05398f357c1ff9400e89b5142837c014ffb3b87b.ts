import {Component, View, CORE_DIRECTIVES} from 'angular2/angular2';

import {RdWidget} from 'components/rd-widget/rd-widget';
import {RdWidgetHeader} from 'components/rd-widget-header/rd-widget-header';
import {RdWidgetBody} from 'components/rd-widget-body/rd-widget-body';

import {CollectionListView} from '../collection-list-view/collection-list-view';
import {FlagsListService} from '../../services/flags_list';

@Component({
    selector: 'flags',
    bindings: [FlagsListService]
})
@View({
    templateUrl: './components/flags/flags.html',
    directives: [CORE_DIRECTIVES, RdWidget, RdWidgetHeader, RdWidgetBody, CollectionListView]
})
export class Flags {
    flags: any[];

    constructor(private _flagsService: FlagsListService) {
        this.flags = _flagsService.all();
    }
}