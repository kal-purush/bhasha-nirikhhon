module amorphaser.Entity {
	/**
	 *		game: Game;
	 *		blade: any;
	 *		isSwinging: boolean; :
	 *		swingDelay: number; : How long the player is stuck to the spot after swinging
	 *		swingArcDistance: number; : How far the sword swings around the player
	 */
	export class BaseEntity extends Phaser.Sprite {
		game: Game;
		updateFunc: () => void;

		constructor(game: Phaser.Game, x: number, y: number, key: string) {
			super(game, x, y, key, 0);
			this.anchor.setTo(0.5, 0.5);
			this.game.physics.enable(this, Phaser.Physics.ARCADE);
		}


	}
}