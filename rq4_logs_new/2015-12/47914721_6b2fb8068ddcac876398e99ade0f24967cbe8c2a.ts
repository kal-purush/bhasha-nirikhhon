/// <reference path="Plant.ts" />

class EggPlant extends Plant {
	
	constructor(spritesheet:Spritesheet, x:number,y:number) {
		super(spritesheet, x, y);
		this.sprites = [
			spritesheet.getUVFromName("egg_0"),
			spritesheet.getUVFromName("egg_1"),
			spritesheet.getUVFromName("egg_2")
		];
		this.growTime = 120;
		this.init();
	}

	update(){
		super.update();
	}
}