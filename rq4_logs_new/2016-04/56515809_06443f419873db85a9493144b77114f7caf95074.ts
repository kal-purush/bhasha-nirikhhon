/// <reference path="../node_modules/angular2/ts/typings/node/node.d.ts"/>
/// <reference path="../node_modules/angular2/typings/browser.d.ts"/>
import { bootstrap } from "angular2/platform/browser";
import { Component } from "angular2/core";
import { Injectable } from "angular2/core";

import {Parser} from "./ts/classes/Parser";
import {ProgressiveLoader} from "./ts/classes/ProgressiveLoader";
import {Renderer} from "./ts/classes/Renderer";
import {HQ} from "./ts/classes/Headquarter";
import {Line} from "./ts/components/Line";
import {LineComponent} from "./ts/components/Line";

//############################ APP #########################################

@Component(
{
	selector: 'reader',
	template: `
	
    <!-- Menu Bar -->
    <nav class="navbar navbar-inverse">
      <div class="container-fluid">
        <div class="navbar-header">
          <a style="color:#DDDDDD" href="#" class="navbar-brand">Angular2 testing</a>
        </div>
        <div>
          <ul class="nav navbar-nav">
            <li>
				<a href="javascript:void(0)" onclick="$('#upload').click()">Open</a>
				<form>
					<input id="upload" type="file" name=test style="visibility:hidden;position:absolute;top:0;left:0;width:0px" (change)="fileChangeEvent($event)">
				</form>
			
			</li>
			<li class="dropdown">
				<a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-haspopup="true" aria-expanded="false">
					New 
					<span class="caret"></span>
				</a>
				<ul class="dropdown-menu">
					<li><a href="javascript:void(0)" (click)="newMap(10,10)">Tiny</a></li>
					<li><a href="javascript:void(0)" (click)="newMap(25,25)">Small</a></li>
					<li><a href="javascript:void(0)" (click)="newMap(50,50)">Normal</a></li>
					<li><a href="javascript:void(0)" (click)="newMap(75,75)">Big</a></li>
					<li><a href="javascript:void(0)" (click)="newMap(90,90)">Huge</a></li>
					<li><a href="javascript:void(0)" (click)="newMap(120,120)">Enormous</a></li>
				</ul>
			</li>
          </ul>
        </div>
      </div>
    </nav>
	<div class='map-container' (click)="click($event)" (contextmenu)="click($event)" [ngClass]="{expanded: toggled, collapsed: !toggled}">
		<div class='map'>
			<line-block *ngFor="#line of getLines()" [line]="line" style="width:{{line.getWidth()}}px"></line-block>
		</div>
	</div>
	
	<build-menu *ngIf="renderer.getLines()" [ngClass]="{expanded: toggled, collapsed: !toggled}">
		<collapse-button (click)="toggle()"></collapse-button>
	</build-menu>
	`,
	directives: [LineComponent],
	providers: [Renderer, ProgressiveLoader, HQ]
}
)
class mainApp {
	
	file : File;
	contentText : string;
	toggled : boolean;
	
	constructor(public renderer: Renderer, public HQ :HQ){
		this.toggled = true;
		this.file = null;
	}
	
	getLines() :Line[]{
		return this.renderer.getLines();
	}
	
	newMap(x:number, y:number) : void{
		var lines : Line[] = [];
		for(var i = 0; i < y; i++){
			var line:Line = new Line(i, x);
			line.complete();
			lines.push(line);
		}
		this.renderer.render(lines);
		
	}
	
	//events
	
	click($event){
		this.HQ.alertOnClick($event);
	}
	
	// file reading
	fileChangeEvent(fileInput: any){
        this.file = (<File[]> fileInput.target.files)[0];
		this.read();
    }
	
	read(): void {
		var reader : FileReader = new FileReader();
		reader.onload  = (e) => {
			var lines = Parser.parse(reader.result);
			this.renderer.render(lines);
		};
		reader.readAsText(this.file);
	}
	
	toggle() :void{
		this.toggled = !this.toggled;
	}
}

bootstrap(mainApp);