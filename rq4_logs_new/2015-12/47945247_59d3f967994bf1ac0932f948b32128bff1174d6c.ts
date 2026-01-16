/// <reference path="../typings/meteor/meteor.d.ts" />
/// <reference path="../typings/phaser/phaser.d.ts" />

module Game {
	export class ExtremeWalkingSimulator {
		game: Phaser.Game;

		constructor(){
			this.game = new Phaser.Game(1280, 720, Phaser.AUTO, 'content', {
				create: this.create,
				preload: this.preload
			});
		}

		preload(){}

		create(){}
	}
}

// window.onload = () => {
// 	var game = new Game.ExtremeWalkingSimulator();
// }

if(Meteor.isClient){
	Template['game'].rendered = function(){
		console.log("Game template onLoad"); 
		this.game = new Game.ExtremeWalkingSimulator();
	}
}