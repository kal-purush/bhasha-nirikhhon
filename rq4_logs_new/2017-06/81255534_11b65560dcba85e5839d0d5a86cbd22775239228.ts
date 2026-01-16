import { Component, OnInit } from '@angular/core';
import { ICellRendererAngularComp } from 'ag-grid-angular/main';

@Component({
	selector: 'app-boolean-renderer',
	template: `{{params.value.value}}`,
	styleUrls: ['./boolean-renderer.component.css']
})

export class BooleanRendererComponent implements ICellRendererAngularComp {
	private params:any;

	agInit(params:any):void {
		this.params = params;
		this.params.eGridCell.addEventListener('click', (this.onClick).bind(this));
	}

	private onClick($event){
		let startEditingParams = {
									rowIndex:this.params.rowIndex,
									colKey: this.params.colDef.field
									};

		this.params.api.startEditingCell(startEditingParams);
	}
}