import { Component } from "angular2/core";
import {NgClass} from 'angular2/common';

import {Cell} from "./Cell";
import {Headquarter} 				from "../services/Headquarter";

@Component(
{
	selector: 'cell-block',
	inputs:['cell'],
	template: `
		<div style="width:{{cellWidth()}}px; height:{{cellHeigth()}}px" class="{{cell.getBuildingName()}}" 
			(mousedown)="mouseDown($event)"
			(mouseup)="mouseUp($event)"
			(mouseenter)="mouseEnter($event)"
			(contextmenu)="rightClick($event)"></div>
		<div [hidden]="!cell.hl" class="hl hl-{{cell.hl}}"
			(mousedown)="mouseDown($event)"
			(mouseup)="mouseUp($event)"
			(mouseenter)="mouseEnter($event)"
			(contextmenu)="rightClick($event)">
				<div class="glyphicon orientation" [ngClass]="{ 'glyphicon-chevron-up' : (hlOrientation() == 'v' ||  hlOrientation() == 'n'),
														    'glyphicon-chevron-right' : (hlOrientation() == 'h' ||  hlOrientation() == 'e'),
															'glyphicon-chevron-down' : hlOrientation() == 's',
														    'glyphicon-chevron-left' : hlOrientation() == 'w'}"></div>
			</div>
	`
}
)
export class CellComponent {

	cell : Cell;

	
	constructor(public HQ : Headquarter){}
	
	cellWidth() {
		return 16*this.cell.getBuilding().width;
	}
	
	cellHeigth() {
		return 16*this.cell.getBuilding().height;
	}
	
	hlOrientation () {
		return this.cell.hlOrientation;
	}
	
	mouseEnter($event){
		this.HQ.alertCellMouseEvent($event, "enter", this.cell);
	}
	
	mouseDown($event){
		this.HQ.alertCellMouseEvent($event, "down", this.cell);
	}
	
	mouseUp($event){
		this.HQ.alertCellMouseEvent($event,"up", this.cell);
	}
	
	rightClick($event){
		this.HQ.alertMainMouseEvent($event, "click");
		$event.preventDefault();
	}
}