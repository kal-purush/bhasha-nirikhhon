/// <reference path="./phaser.d.ts"/>
module CityDefender {
    export class Preloader extends Phaser.State {

	preloadBar: Phaser.Sprite;


	preload() {

//	    this.preloadBar = this.add.sprite(200, 250, 'preload');
//	    this.load.setPreloadSprite();
	    this.load.image('city', 'assets/images/city.png');
	    
	}

	create () {
	    var tween = this.add.tween(this.preloadBar).to({alpha: 0}, 1000, Phaser.Easing.Linear.None, true);
	    tween.onComplete.add(this.startMainMenu, this);

	}

	startMainMenu(){
	    this.game.state.start('MainMenu', true, false);
	}
		
    }

}