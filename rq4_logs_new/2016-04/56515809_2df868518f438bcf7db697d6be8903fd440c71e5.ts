import {Injectable} from "angular2/core";

import {Line} from "../components/Line";
import {Cell} from "../components/Cell";
import {Renderer} from "./Renderer";
import {Service} from "./Service";

@Injectable()
export class RoadService implements Service {

	currentCell : Cell;
	originCell : Cell;
	
	building : string;
	
	highlightedCells : Cell[];
	pathOK : boolean;
	
	constructor(public renderer :Renderer){
		this.highlightedCells = [];
		this.building = '-';
		this.pathOK = false;
	}
	
	init( args? : string[] ) :void {
		if(args && args.length > 0){
			var name = args[0];
			this.building = Cell.getCharFromName(name);
			console.log(`RoadService initialized with ${name} (${this.building})`);
		}
	}
	
	alertCellMouseEvent($event, action :string, cell :Cell) :void {
	
		if( action == "enter" ){
			this.alertOnEnter( $event , cell );
		}
		else if( action == "up" ){
			this.alertOnMouseUp( $event , cell );
		} 
		else if( action == "down" ){
			this.alertOnMouseDown( $event , cell );
		}
		else {
			console.log(`Error, unknown action ${action} for provider RoadService`);
		}
	}
	
	reset() :void {
		
		this.highlightedCells.forEach(
			(cell) => {cell.highlight(null)}
		);
		this.highlightedCells = [];
		this.originCell = null;
		this.currentCell = null;
	}
	
	alertOnEnter($event, cell : Cell) :void {
		this.currentCell = cell;
		if($event.buttons == 1){
			this.pathOK = this.highlighCells();
		}
	}
	
	alertOnMouseUp($event, cell : Cell) :void {
		if($event.button == 0){
			//left click
			if(cell && this.pathOK){
				this.highlightedCells.forEach(
					(hlCell) => {
						hlCell.char = this.building;
						this.renderer.renderCell(hlCell, true);
					}
				);
			}
			this.reset();
		} else if($event.button == 1){
			this.reset();
		}
	}
	
	alertOnMouseDown($event, cell : Cell) :void {
		if($event.button == 0){
			this.originCell = cell;
			this.pathOK = this.highlighCells();
		}
	}
	
	highlighCells() :boolean {
		var pathOK = true;
		var startX = this.originCell.lineIndex;
		var startY = this.originCell.colIndex;
		var endX = this.currentCell.lineIndex;
		var endY = this.currentCell.colIndex;
		
		this.highlightedCells.forEach(
			(cell) => {cell.highlight(null)}
		);
		this.highlightedCells = [];
		
		var cells = this.renderer.getCellsInPath(
			this.originCell.lineIndex,
			this.originCell.colIndex,
			this.currentCell.lineIndex,
			this.currentCell.colIndex );
		
		cells.forEach(
			(cell) => {
				this.highlightedCells.push(cell);
				if(cell.isEmpty()){
					cell.highlight("green");
				} else {
					cell.highlight("red");
					pathOK = false;
				}
			}
		);
		return pathOK;
	}
	
}