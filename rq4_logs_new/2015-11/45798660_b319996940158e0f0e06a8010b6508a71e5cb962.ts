module Jarl {
	export enum TypeOfTile {Jarl, Freeman, Undefined};
	
	export interface Tile {
		color : Color;
		tileType : TypeOfTile; 
	}
}