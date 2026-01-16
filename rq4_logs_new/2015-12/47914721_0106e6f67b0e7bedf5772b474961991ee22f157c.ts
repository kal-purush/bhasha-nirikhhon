/// <reference path="Stage.ts" />
/// <reference path="Tile.ts" />


class PlayStage extends Stage {

	tiles: Array<Tile> = new Array();
	button_dirt: Plane;

	constructor() {
		super();
	}

	init(){
		//init map
		for (var x = 0; x < (640 / 32); x++){
			for (var y = 0; y < (480 / 32); y++){
				this.tiles.push(new Tile(this.spritesheet, "grass_0", 16+x*32, 16+y*32));
			}
		}

		this.button_dirt = new Plane(400, 480 - 32, 11,16, 16, Color.WHITE, this.spritesheet.getUVFromName("dbutton"));
	}

	update() {
		for (var i = 0; i < this.tiles.length; i++){
			this.tiles[i].update();
		}
	}

	render(scene: Scene) {
		for (var i = 0; i < this.tiles.length; i++){
			scene.addPlane(this.tiles[i].plane);
		}

		scene.addPlane(this.button_dirt);
	}

	mouseMove(e:MouseEvent){
		console.log(e.offsetX);
	}

	mouseClick(e:MouseEvent){
		console.log("Click")
	}


}