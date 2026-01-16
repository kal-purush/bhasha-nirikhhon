import { Component } from "angular2/core";
import {Headquarter} 			from "../services/Headquarter";
import {Line} 				from "./Line";

declare var saveAs: any;
declare var html2canvas: any;

@Component({
	selector: '[templates-menu]',
	inputs : ['templates'],
	directives: [],
	template: `
			<ng-content></ng-content>
			<ul class="dropdown-menu">
				<li *ngFor="#template of templates"><a href="javascript:void(0)" (click)="click(template)">{{template.getName()}}
				( {{template.getDimension()}} )</a></li>
				<li *ngIf="!templates || templates.length == 0"><a>No templates yet</a></li>
			</ul>
	`
})
export class TemplatesMenu {

	templates : Array<Template>;

	constructor(public HQ : Headquarter){
		
	}

	click(template:Template){
		console.log(template.content);
		var cells = [];
		template.content.forEach(
			(line) => {
				line.cells.forEach(
					(cell) => {
						cells.push(cell);
					}
				);
			}
		);

		this.HQ.notifySelection({
			name : 'copy',
			service : 'copy',
			data : cells
		});
	}
}

export class Template {

	constructor(public name : string, public content:Line[]){

	}

	getName() :string{
		return this.name.substring(0, this.name.lastIndexOf("."));
	}

	getDimension() :string{
		return `${this.content.length} x ${this.content[0].cells.length}`;
	}
}