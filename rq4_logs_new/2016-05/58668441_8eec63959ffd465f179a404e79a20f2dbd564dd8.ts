import {Component} from '@angular/core';
import {MD_LIST_DIRECTIVES} from '@angular2-material/list';

import {ActionService} from "../shared/actions/ActionService";
import * as PiStation from "../../../PiStation";
import {Observable} from "rxjs/Observable";

@Component({
    selector : 'module-list',
    directives: [MD_LIST_DIRECTIVES],
	templateUrl: 'app/+modules/module-list.component.html'
})
export class ModuleListComponent {
    public modules : Observable<PiStation.Module[]>;

	constructor(private actionService : ActionService){
        this.modules = this.actionService.getAllModules();
	}
}