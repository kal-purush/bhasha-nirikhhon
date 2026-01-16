/**
 * Created by griga on 2/23/16.
 */


module M22Shooter{
    export class Enemy extends Phaser.Sprite{

        constructor(game: Phaser.Game){
            super(game, 0, 0, 'enemy'+ game.rnd.integerInRange(1, 3))

            // Set its pivot point to the center of the bullet
            this.anchor.setTo(0.5, 0.5);

            // Enable physics on the bullet
            game.physics.enable(this, Phaser.Physics.ARCADE);

            game.add.existing(this);
            // Set its initial state to "dead".
            this.kill();
        }

    }
}