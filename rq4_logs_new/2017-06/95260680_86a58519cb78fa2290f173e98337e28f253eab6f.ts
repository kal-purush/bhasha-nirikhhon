import GameObject from "./salad/gameobject"
import ParticleSystem from "./salad/particlesystem"
import Obstacles from "./obstacles"

class Bullet extends GameObject {
    trail:ParticleSystem;
    velocity:number;
    alive:boolean;
    x:number;
    y:number;
    explosion:ParticleSystem;
    obstacles:Obstacles;
    deathOffset:number;
    constructor(x:number = 0, y:number = 0) {
        super();
        this.x = x;
        this.y = y;
        this.velocity = 0.1;
        this.alive = true;
        this.trail = new ParticleSystem({
            systemLifeTime: 0,
            horizontal: 0,
            lifeTime: 800,
            particleCount: 40, 
            texture: "images/particle_small.png", 
            variation: 0.01,
            vertical: 0.1,
            x: 0, 
            y: 0
        });      
        this.add(this.trail);  
        this.on('collides', (obstacles:Obstacles) => {
            this.explode(obstacles);
        });
    }
    explode(obstacles:Obstacles) {
        this.obstacles = obstacles;
        if(this.explosion) {
            return
        }
        this.explosion = new ParticleSystem({
            systemLifeTime: 500,
            horizontal: 0,
            lifeTime: 1000,
            particleCount: 50, 
            texture: "images/particle.png", 
            variation: 0.1,
            vertical: 0.2,
            x: this.x, 
            y: this.y
        });
        this.explosion.fill();
        this.deathOffset = obstacles.offset;
        this.explosion.on("eol", () => {
            console.log('explosion ended');
            delete this.explosion;
            this.emit('eol');
        });
        this.emit('uncollide');
        this.alive = false;
    }
    update(delta:number) {
        super.update(delta);
        if(this.alive) {
            this.y -= Math.floor(this.velocity * delta);
            if(this.y < 0) {
                this.alive = false;
                this.emit("eol");
            }
        } else {
            if(this.explosion && this.obstacles) {
                let offset = this.obstacles.offset - this.deathOffset;
                this.explosion.y = this.y + offset - 10;
                this.explosion.update(delta);
            }            
        }
    }
    draw(context:CanvasRenderingContext2D) {        
        if(this.alive) {
            context.save();
            context.translate(this.x, this.y);
            super.draw(context); 
            context.restore();
        } else {
            if(this.explosion) {
                this.explosion.draw(context);
            }
        }
    }
}

export default Bullet;