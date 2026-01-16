import {Component} from '@angular/core';

export interface FilterCell {
  clear: any;
}

@Component({
  selector: 'filter-cell-component',
  template: '<div></div>'
})
export class FilterCellComponent implements FilterCell {
  clear() { }
}