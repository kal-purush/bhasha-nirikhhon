import { Component } from "angular2/core";
import {Headquarter} 			from "../services/Headquarter";
import {HighlightDirective} 	from "../directives/highlight.directive";

@Component({

	selector: 'context-menu-holder',
	directives :[HighlightDirective],
	host:{
    	'(document:click)':'clickedOutside()',
	},
	template: `
		<div [ngStyle]="locationCss" class="container">
	      <div class="panel panel-primary context-menu">
	      	<div class="panel-heading">Context Menu</div>

		        <div *ngFor="#option of options" class="menu-option panel-default">
		        	<div class="panel-heading" (mouseenter)="enterOption($event, option)"
		        	(click)="clickOnOption($event, option)" 
		        	[class-switch]="false">
		        		{{option.name}}
		        </div>
	        </div>

	        <div class="context-menu panel panel-primary" [ngStyle]="locationCssSubmenu">
	        	<div *ngFor="#subOption of subOptions" class="menu-option panel-default">
		        	<div class="panel-heading" (click)="clickOnOption($event, subOption)" [class-switch]="false">
		        			{{subOption.name}}
		        	</div>
	        	</div>
	        </div>
	      </div>
	    </div>
	`
})
export class ContextMenuHolder {

	options : Array<any>;
	subOptions :Array<any>;
	currentOption : any;
	isShown :boolean;
	mouseLocation : {left:number, top:number} = {left:0,top:0};
	submenuLocation : {left:number, top:number} = {left:0,top:0};
	contextMenus : any;

	constructor(public HQ : Headquarter){
		this.isShown = false;
		this.HQ.contextMenuChange$.subscribe(
			(input) => {
				var event = input.event;
				this.initMenus(input)
				this.showMenu(event);
			}
		);
	}

	get locationCss(){ 
		return {
		  'position':'fixed',
		  'display':this.isShown ?  'block':'none',
		  'left':this.mouseLocation.left + 'px',
		  'top':(this.mouseLocation.top - 25) + 'px'
		};
	}

	get locationCssSubmenu(){
		return {
		  'position':'fixed',
		  'display':(this.subOptions && this.subOptions.length > 0) ?  'block':'none',
		  'left':this.submenuLocation.left + 'px',
		  'top':(this.submenuLocation.top) + 'px'
		};
	}

	enterOption($event, option :any){
		this.currentOption = option;
		if(!option.subOptions){
			this.subOptions = [];
			return;
		}
		this.subOptions = option.subOptions;
		var optionPos = $event.target.getBoundingClientRect();
		this.submenuLocation.top = optionPos.top;
		this.submenuLocation.left = optionPos.right;
	}

	clickedOutside(){
		this.isShown= false;
	}

	clickOnOption($event:MouseEvent, option :any){
		if(!option.service){
			//menu with submenus
			$event.preventDefault();
			return;
		}
		if($event && $event.button == 0){
			this.HQ.notifySelection({
				name : option.service,
				service : option.service,
				data : option.data
			});
		}
		this.subOptions = [];
		this.currentOption = null;
	}

	showMenu(event){
		this.isShown = true;
		this.mouseLocation = {
		  left:event.clientX,
		  top:event.clientY
		}
	}

	initMenus(input) :void{
		this.contextMenus = {

			"select" : [
				{
					name : 'copy',
					service : 'copy',
					data : input.data
				},
				{
					name : 'move',
					service : 'move',
					data : input.data
				},
				{	
					name : 'copy & rotate',

					subOptions : [
						{
							name : 'vertically',
							service : 'copyAndRotate',
							data : {
								cells : input.data,
								rotation : 'vertical'
							}
						},
						{
							name : 'horizontally',
							service : 'copyAndRotate',
							data : {
								cells : input.data,
								rotation : 'horizontal'
							}
						}
					]
				}
			],
		}
		this.options = this.contextMenus[input.source];
	}
	
}