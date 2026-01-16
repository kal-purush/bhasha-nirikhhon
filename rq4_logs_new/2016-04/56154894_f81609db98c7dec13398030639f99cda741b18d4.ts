import {Component, OnInit} from 'angular2/core';
import {Nodule} from './nodule';
import {NoduleService} from './nodule.service';

@Component({
    selector: 'my-nodules',
    templateUrl: 'app/nodule.list.html'
})
export class NodulesComponent implements OnInit {
    title: "Nodules App";
    nodules: Nodule[];
    
    ngOnInit() {
        this.getNodules();
    }
    
    constructor(private _noduleService: NoduleService) {        
    }
    
    getNodules() {
        this._noduleService.getNodules().then(nodules => this.nodules = nodules);
    }
 }