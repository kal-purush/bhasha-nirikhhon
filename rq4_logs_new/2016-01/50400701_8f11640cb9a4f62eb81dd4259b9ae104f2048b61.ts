import {Component} from 'angular2/core';
import {BsGridComponent} from './ng2-bootstrap-grid/ng2-bootstrap-grid';
import {GridOptions,GridColumn} from './ng2-bootstrap-grid/gridOptions';
import {PersonData} from './person'

@Component({
    selector: 'app',
    templateUrl: 'app/app.html',
    styleUrls:['app/app.css'],
    directives: [BsGridComponent]
})
export class App {
    public gridOptions = new GridOptions(true, [new GridColumn('ID','id'),new GridColumn('Name','name')], [new PersonData('1','Coral')]);

}