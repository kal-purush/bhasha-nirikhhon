import {Component} from 'angular2/core';
import {GridOptions} from './gridOptions';

@Component({
    selector: 'bs-grid',
    styleUrls:['app/ng2-bootstrap-grid/grid.css'],
    template: `
    <table class='table table-bordered table-hover table-striped'>
      <thead>
        <tr>
            <th *ngIf='options.enableRowSelection' class='grid-checkbox-cell'>
                <input type='checkbox'>
            </th>
            <th *ngFor="#column of options.columns">{{column.displayName}}</th>
        </tr>
      </thead>
      <tbody>
        <tr *ngFor="#item of options.data">
            <td *ngIf='options.enableRowSelection' class='grid-checkbox-cell'><input type='checkbox'></td>
            <td *ngFor="#col of options.columns">
                {{item[col.field]}}
            </td>
        </tr>
      </tbody>
    </table>
    `,
    inputs: ['options']
})
export class BsGridComponent {
    public options: GridOptions;
}