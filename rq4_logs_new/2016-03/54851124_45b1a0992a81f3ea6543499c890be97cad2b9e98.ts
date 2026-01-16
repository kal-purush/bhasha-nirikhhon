import {Component, View, CORE_DIRECTIVES} from 'angular2/angular2';

import {RdLoading} from 'components/rd-loading/rd-loading';
import {RdWidget} from 'components/rd-widget/rd-widget';

import {RdWidgetHeader} from 'components/rd-widget-header/rd-widget-header';
import {RdWidgetBody} from 'components/rd-widget-body/rd-widget-body';

import {JumperListView} from 'components/jumpers/jumper-list-view/jumper-list-view';
import {JumperListService} from 'services/jumper_list';

@Component({
  selector: 'jumpers',
  appInjector: [JumperListService]
})
@View({
  templateUrl: './components/jumpers/jumpers.html',
  directives: [CORE_DIRECTIVES, RdWidget, RdWidgetHeader, RdWidgetBody, RdLoading, JumperListView]
})
export class Jumpers {
  servers:any[];
  jumperListService:JumperListService;

  constructor() {
    this.jumperListService = new JumperListService();/*TODO: Inject*/
    this.jumpers = this.jumperListService.all();
  }
}