module Jarl {
	export interface PlayerInterface {
		getAvailableTiles() : Array<Tile>;
		addLostTile(lostTile : Tile) : Number;
		drawTileFromBag() : Array<Tile>; //Draws a random Tile from the bag and adds it to availableTiles and returns availableTiles
	}
	
	export class Player implements PlayerInterface{
		private lostTiles : Array<Tile>;
		private availableTiles : Array<Tile>;
		private tilesInBag : Array<Tile>;
		
		private jarlTile : Tile;
		private freeman : Tile;
		
		constructor(color_ : Color) {
			this.jarlTile = {color : color_, sort : SortOfTile.Jarl};
			this.freeman =  {color : color_, sort : SortOfTile.Freeman};
			this.tilesInBag = [this.freeman, this.freeman, this.freeman, this.freeman];
			this.availableTiles = [this.jarlTile, this.freeman, this.freeman];
			this.lostTiles = [];
		};
		
		public getAvailableTiles() : Array<Tile> {
			return this.availableTiles;
		};
		
		public addLostTile(lostTile : Tile) : Number {
			var nbrOfLostTiles : Number = this.lostTiles.push(lostTile);
			return nbrOfLostTiles;
		};
		
		public drawTileFromBag() : Array<Tile> {
			var tile : Tile[];
			var index = Math.floor(Math.random() * this.tilesInBag.length);
			tile = this.tilesInBag.splice(index, 1); // returns only one tile, hence tile[0] can be used in next step
			this.availableTiles.push(tile[0]);
			
			return this.availableTiles; 
		};
	};
};