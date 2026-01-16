var canvas = <HTMLCanvasElement> document.getElementById("myCanvas");
var ctx = canvas.getContext("2d");

class playerInput {
    public right:boolean = false;
    public left:boolean = false;
    public up:boolean = false;
    public down:boolean = false;
    public fire:boolean = false;
}

class keyInput extends playerInput {
    private rightKey:number = 39;
    private leftKey:number = 37;
    private upKey:number = 38;
    private downKey:number = 40;
    private fireKey:number = 32;

    constructor() {
        super();
        document.addEventListener("keydown", this.keyHandler.bind(this, true), false);
        document.addEventListener("keyup", this.keyHandler.bind(this, false), false);
    }

    keyHandler(keydown:boolean, e) {
        if (e.keyCode == this.leftKey) {
            this.left = keydown;
        }
        else if (e.keyCode == this.upKey) {
            this.up = keydown;
        }
        if (e.keyCode == this.rightKey) {
            this.right = keydown;
        }
        else if (e.keyCode == this.downKey) {
            this.down = keydown;
        }
        if (e.keyCode == this.fireKey) {
            this.fire = keydown;
        }
    }
}


class GameObject {
    protected startX:number;
    protected startY:number;
    public x:number;
    public y:number;
    public width:number;
    public height:number;
    protected visible:boolean;
    protected ctx:CanvasRenderingContext2D;
    protected active:boolean = false;

    constructor(protected canvas:HTMLCanvasElement) {
        this.canvas = canvas;
        this.ctx = canvas.getContext("2d");
        this.initialise();
    }

    spawn(x:number = 0, y:number = 0) {
    }

    isActive() {
        return this.active
    }

    initialise():void {
        this.startX = canvas.width / 2;
        this.startY = canvas.height / 2;
        this.x = this.startX;
        this.y = this.startY;
        this.visible = true;
    }

    draw():void {
    }

    updatePosition():void {
    }

    destroy():void {
        this.active = false;
        this.visible = false;
    }
}

class Projectile extends GameObject {
    private colour:string = randomColour();
    public static rate:number = 200;
    public static radius = 5;

    constructor(protected canvas:HTMLCanvasElement) {
        super(canvas);
        this.visible = false;
        this.height = Projectile.radius * 2; //used for collision detection
        this.width = Projectile.radius * 2;
    }

    spawn(x, y) {
        this.active = true;
        this.visible = true;
        this.x = x;
        this.y = y;
    }

    draw():void {
        if (this.visible) {
            this.ctx.beginPath();
            this.ctx.arc(this.x, this.y, Projectile.radius, 0, Math.PI * 2);
            this.ctx.fillStyle = this.colour;
            this.ctx.fill();
            this.ctx.closePath();
        }
    }

    updatePosition() {
        if (this.active) {
            this.x += 9;
        }
        if (this.x > this.canvas.width) {
            this.active = false;
            this.visible = false;
        }
    }
}

class Enemy extends GameObject {
    private colour = randomColour();

    constructor(canvas:HTMLCanvasElement) {
        super(canvas);
        this.width = canvas.width / 15;
        this.height = canvas.height / 20;
    }

    draw() {
        if (this.visible) {
            this.ctx.beginPath();
            this.ctx.rect(this.x, this.y, this.width, this.height);
            this.ctx.fillStyle = this.colour;
            this.ctx.fill();
            this.ctx.closePath();
        }
    }

    spawn() {
        this.active = true;
        this.visible = true;
        this.x = canvas.width;
        this.y = canvas.height * Math.random();
        this.colour = randomColour();
    }

    updatePosition() {
        if (this.active) {
            this.x -= 7;
            if (this.x < 0) {
                this.active = false;
                this.visible = false;
            }
        }
    }
}

class Ship extends GameObject {
    private input:playerInput = new keyInput();
    public projectiles:Projectile[] = [];
    private lastShot:number = 0;

    constructor(canvas:HTMLCanvasElement) {
        super(canvas);
        this.width = canvas.width / 10;
        this.height = canvas.height / 15;
    }

    draw():void {
        this.ctx.beginPath();
        this.ctx.rect(this.x, this.y, this.width, this.height);
        this.ctx.fillStyle = "#0095DD";
        this.ctx.fill();
        this.ctx.closePath();
        this.projectiles.forEach((projectile)=> {
            projectile.draw()
        });
    }

    updatePosition() {
        if (this.input.right && this.x < this.canvas.width - this.width) {
            this.x += 7;
        }
        else if (this.input.left && this.x > 0) {
            this.x -= 7;
        }
        if (this.input.down && this.y < this.canvas.height - this.height) {
            this.y += 7;
        }
        else if (this.input.up && this.y > 0) {
            this.y -= 7;
        }
        if (this.input.fire && this.lastShot + Projectile.rate < Date.now()) {
            this.fireNextProjectile();
            this.lastShot = Date.now();
        }
        this.projectiles.forEach((projectile)=> {
            projectile.updatePosition()
        });
    }

    fireNextProjectile() {
        for (var i = 0; i < this.projectiles.length; i++) {
            if (!this.projectiles[i].isActive()) {
                this.projectiles[i].spawn(this.x + this.width, this.y + (this.height / 2));
                return;
            }
        }
        let newProj = new Projectile(this.canvas);
        newProj.spawn(this.x + this.width, this.y + (this.height / 2));
        this.projectiles.push(newProj);
        console.log("Projectiles " + this.projectiles.length);
    }
}

function randomColour() {
    return '#' + (function co(lor) {
            return (lor +=
                [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 'a', 'b', 'c', 'd', 'e', 'f'][Math.floor(Math.random() * 16)])
            && (lor.length == 6) ? lor : co(lor);
        })('');
}

class Game {
    ship:Ship = new Ship(canvas);
    enemies:Enemy[] = [];
    private running:boolean = false;
    constructor(){
        document.addEventListener("keydown",(key)=>{
           if(key.keyCode == 49) {
               this.start();
           }
        }, false);
    }

    spawnEnemy() {
        if (Math.random() * 100 > 95) {
            let enemy;
            for (let i = 0; i < this.enemies.length; i++) {
                if (!this.enemies[i].isActive()) {
                    enemy = this.enemies[i];
                    break;
                }
            }
            if (!enemy) {
                enemy = new Enemy(canvas);
                this.enemies.push(enemy);
            }
            enemy.spawn();
        }
    }

    gameLoop() {
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        this.ship.updatePosition();
        this.ship.draw();
        this.enemies.forEach((enemy)=> {
            enemy.updatePosition();
            enemy.draw();
        });
        this.spawnEnemy();
        this.collisionDetection();
        if (this.running) {
            requestAnimationFrame(()=> {
                this.gameLoop()
            });
        }
    }

    start() {
        this.running = true;
        this.gameLoop();
    }

    stop() {
        this.running = false;
    }

    collisionDetection() {
        this.enemies.forEach((enemy)=> {
            if (enemy.isActive()) {
                this.ship.projectiles.forEach((projectile)=> {
                        if (projectile.isActive() && Game.collision(projectile, enemy)) {
                            enemy.destroy();
                            projectile.destroy();
                        }
                    }
                );
                if (Game.collision(this.ship, enemy)) {
                    enemy.destroy();
                    this.ship.destroy();
                    this.stop();
                }
            }
        });
    }

    static collision(a:GameObject, b:GameObject) {
        return !!(a.x < b.x + b.width &&
        a.x + a.width > b.x &&
        a.y < b.y + b.height &&
        a.height + a.y > b.y);
    }
}

let game = new Game();
game.start();