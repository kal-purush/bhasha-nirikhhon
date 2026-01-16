/**
 * Firing system (named Weapon because it makes sense :D)
 * In the future, will also be used to represent enemy firing system (subclasses?)
 */
export class Weapon {
    public firing = false;

    private fireInterval = 200;
    private lastFired = 0;

    public update() {
        if (this.canFire()) this.fire();
    }

    private canFire(): boolean {
        var now = Date.now();
        var diff = now - this.lastFired;
        var fire = diff > this.fireInterval;
        return this.firing && fire;
        //return this.firing && this.date.getTime() - this.lastFired > this.fireInterval;
    }

    private fire(): void {
        console.log("FIRE!!!")
        this.lastFired = Date.now();
    }

}