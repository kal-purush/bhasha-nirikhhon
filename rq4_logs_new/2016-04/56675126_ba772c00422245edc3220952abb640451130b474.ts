import {Component} from 'angular2/core';
import {DataService} from "./data.service";

@Component({
    selector: 'comp-1',
    template: `
        <h2>Component 1</h2>
        <div>
            <button (click)="onGetData()">Get random data</button>
            <p>Random Data: {{data}}</p>
            <input type="text" #input>
            <button (click)="onAddItem(input.value)">Add</button>
        </div>
    `,
    // providers: [DataService]
})
export class Comp1Component{
    data: string;

    constructor(private _dataService: DataService) {}

    onGetData() {
        this.data = this._dataService.getRandomData();
    }
    onAddItem(data: string) {
        this._dataService.inserData(data);
    }
}