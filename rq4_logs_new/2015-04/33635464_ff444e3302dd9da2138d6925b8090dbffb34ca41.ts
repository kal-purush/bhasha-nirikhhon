///<reference path='../Game.ts' />

module entity {
	
	export class Worm extends Entity {
		
		// Called when the entity is added to the world
		added(): void {}
		
		// Called when the entity is removed from the world
		removed():void {}
		
		resolveMovementX(dx:number): void {
			this.residueX += dx;
			dx = Math.round(this.residueX);
			this.residueX -= dx;
			
			var sign = dx > 0 ? 1 : -1;
			
			while (dx != 0) {
				if (this.collideMap(this.x+sign, this.y, false)) {
					if (!this.collideMap(this.x+sign, this.y-1, false)) {
						this.y--;
					}
					else if (!this.collideMap(this.x+sign, this.y-2, false)) {
						this.y-=2;
					}
					else {
						this.velX *= -this.getBounce();
						break;
					}
				}
				else if(this.isOnFloor()) {
					if (!this.collideMap(this.x+sign, this.y+1, false)) {
						this.y++;
					}
					else if (!this.collideMap(this.x+sign, this.y+2, false)) {
						this.y+=2;
					}
				}
				this.x += sign;
				dx -= sign;
				this.onFloor = -1;
			}
		}
		
		tick(): void {
			super.tick();
			//do worm specific things here
		}
		
		render():void {
			super.render();
			//animation shenanigans
		}
		
		getBounce(): number {
			return super.getBounce();
			//make worm bounce a bit if falling
		}
		
	}
	
}