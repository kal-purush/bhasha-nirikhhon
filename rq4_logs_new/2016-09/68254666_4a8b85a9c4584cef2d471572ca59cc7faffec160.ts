module amorphaser.Entity {
	/**
	 *		game: Game;
	 *		blade: any;
	 *		isSwinging: boolean; :
	 *		swingDelay: number; : How long the player is stuck to the spot after swinging
	 *		swingArcDistance: number; : How far the sword swings around the player
	 */
	export class TestEnemy extends Phaser.Sprite {
		game: Game;

		constructor(game: Phaser.Game, x: number, y: number) {
			super(game, x, y, 'enemy', 0);
			this.anchor.setTo(0.5, 0);
			this.game.physics.enable(this, Phaser.Physics.ARCADE);
			game.add.existing(this);
		}

	}
}