///<reference path='../Game.ts' />
/// <reference path='../entity/Worm.ts' />

module entity {
	import Worm = entity.Worm;
	
	export class BazookaProjectile extends Entity {
		tick(): void {
			super.tick();
			if (this.justCollided) {
				this.world.explosion(this.x, this.y, 20, function (e:Entity) {
					if (e.type === "Worm") {
						var xDist = e.x + e.width/2;
						var yDist = e.y + e.height/2;
						var dist = Math.sqrt(xDist*xDist + yDist*yDist);
						if(dist <= 5) dist = 0;
						else dist -= 5;
						e.hurt(Math.round(Math.sqrt(15-dist)*11.62));
					}
				}, function (e:Entity) {
					//knockback entities
				});
				this.world.remove(this);
			}
		}
		
		render():void {
			var c = this.world.context;
			c.fillStyle = "BLUE";
			c.fillRect(this.x, this.y, this.width, this.height);
		}
	}
}