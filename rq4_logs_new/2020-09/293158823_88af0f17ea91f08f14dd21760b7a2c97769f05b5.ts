import { ValueFormatterParams } from '@ag-grid-community/all-modules';
import { ICellEditorAngularComp } from '@ag-grid-community/angular';
import { Component } from '@angular/core';

@Component({
  selector: 'app-product-quantity-cell',
  template: `
    <ul>
      <li *ngFor="let item of items">{{ item[0] }} - {{ item[1] }}</li>
    </ul>
  `,
  styleUrls: ['./item-list-cell.component.scss'],
})
export class ProductCellComponent implements ICellEditorAngularComp {
  public items: Array<[string, string]> = [];

  agInit(params: ValueFormatterParams): void {
    this.items = params.value;
  }

  getValue() {
    return '';
  }
}