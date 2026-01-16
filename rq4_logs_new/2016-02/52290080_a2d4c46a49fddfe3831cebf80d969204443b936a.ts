/**
 * Created by griga on 2/23/16.
 */
"use strict";

module M22Shooter{
    export class Gun extends Phaser.Group{

        static SHOT_DELAY: number = 100
        static BULLET_SPEED: number = 500
        static NUMBER_OF_BULLETS: number = 20

        lastBulletShotAt: number
        
        

        constructor(public game: Phaser.Game, public owner: Phaser.Sprite){
            super(game);
            for(var i = 0; i < Gun.NUMBER_OF_BULLETS; i++) {
                // Create each bullet and add it to the group.
                var bullet = this.game.add.sprite(0, 0, 'bullet1');
                this.add(bullet);

                // Set its pivot point to the center of the bullet
                bullet.anchor.setTo(0.5, 0.5);

                // Enable physics on the bullet
                game.physics.enable(bullet, Phaser.Physics.ARCADE);

                game.add.existing(this);
                // Set its initial state to "dead".
                bullet.kill();
            }

        }

        shoot() {
            // Enforce a short delay between shots by recording
            // the time that each bullet is shot and testing if
            // the amount of time since the last shot is more than
            // the required delay.
            if (this.lastBulletShotAt === undefined) this.lastBulletShotAt = 0;
            if (this.game.time.now - this.lastBulletShotAt < Gun.SHOT_DELAY) return;
            this.lastBulletShotAt = this.game.time.now;



            // Get a dead bullet from the pool
            let bullet = this.getFirstDead();

            // If there aren't any bullets available then don't shoot
            if (bullet === null || bullet === undefined) return;

            // Revive the bullet
            // This makes the bullet "alive"
            bullet.revive();


            // Gun should kill themselves when they leave the world.
            // Phaser takes care of this for me by setting this flag
            // but you can do it yourself by killing the bullet if
            // its x,y coordinates are outside of the world.
            bullet.checkWorldBounds = true;
            bullet.outOfBoundsKill = true;

            // Set the bullet position to the gun position.
            bullet.reset(this.owner.x, this.owner.y);
            bullet.rotation = this.owner.rotation;

            
            // Shoot it in the right direction
            bullet.body.velocity.x = Math.sin(bullet.rotation) * Gun.BULLET_SPEED;
            bullet.body.velocity.y = -1 * Math.cos(bullet.rotation) * Gun.BULLET_SPEED;
        }
    }
}