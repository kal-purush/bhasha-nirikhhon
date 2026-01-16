/// <reference path="../../typings/meteor/meteor.d.ts" />
/// <reference path="../../typings/phaser/phaser.d.ts" />

eval('var GameFromScratch = (this.GameFromScratch || (this.GameFromScratch = {}))'); // https://doctorllama.wordpress.com/2015/04/21/meteor-typescript-classes-and-modules/

module GameFromScratch {
	export class MyScene extends Phaser.Sprite {
		game: Phaser.Game;
		nextFrame: Phaser.Sprite;

		constructor(game:Phaser.Game, x:number, y:number){
			super(game, x, y, "scene", 0);
			this.nextFrame = new Phaser.Sprite(this.game, this.width, 0, "scene", 0);
			this.game.add.existing(this.nextFrame);
		}
	}
}