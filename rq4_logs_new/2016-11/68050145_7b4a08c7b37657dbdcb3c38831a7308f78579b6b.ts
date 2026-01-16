import { EventEmitter }     from '@angular/core';
import { Subject }          from 'rxjs/Subject';
import { SimulationMessage }from '../models/simulation-message';
export class SimulationEventEmitter {
    private simulationEventSubj = new Subject<any>();
    public simulationEvent = this.simulationEventSubj.asObservable();
    constructor () {
    }
    public raise(value:SimulationMessage) {
        this.simulationEventSubj.next(value);
    }
}