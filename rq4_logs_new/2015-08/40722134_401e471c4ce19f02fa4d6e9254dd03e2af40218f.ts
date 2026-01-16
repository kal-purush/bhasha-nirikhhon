/// <reference path="game.ts" />
/// <reference path="player.ts" />

class FieldDrawer {
    draw(thing: Thing, canvas: HTMLCanvasElement) {
        var ctx = canvas.getContext("2d");
        ctx.fillStyle = "green";
        ctx.fillRect(thing.pos.x, thing.pos.y, 720, 320);
        for (var i = 0; i < 11; i++) {
            if ((i == 0) || (i == 10)) {
                ctx.strokeStyle = "yellow";
                ctx.lineWidth = 3;
            } else {
                ctx.strokeStyle = "white";
                ctx.lineWidth = 1;
            }
            ctx.beginPath();
            ctx.moveTo(60 + i*60,0);
            ctx.lineTo(60 + i*60, 320);
            ctx.stroke();
        }
    }
}

class Game {
    constructor(private canvas: HTMLCanvasElement){
        this.scene.things.push(new Thing(new Vector.Vector(0,0,0), Things.Field,null,new FieldDrawer()));
        this.scene.things.push(new Thing(
            new Vector.Vector(60,6,0), Things.Player,
            new RouteFollower(
                [new Vector.Vector(300, 6, 0), new Vector.Vector(300, 240, 0), new Vector.Vector(60, 240, 0)],
                0.8), new PlayerDrawer('red')));
         var ball_carrier: Thing;
         this.scene.things.push(ball_carrier = new Thing(
            new Vector.Vector(660,6,0), Things.Player,
            new RouteFollower(
                [new Vector.Vector(420, 6, 0), new Vector.Vector(420, 240, 0), new Vector.Vector(660, 240, 0)],
                0.8), new PlayerDrawer('blue')));    
         ball_carrier.possessions.push(new Thing(new Vector.Vector(0,0,0), Things.Ball))
        window.setInterval(this.update, 16.666);
    }
    update = () => { 
        this.scene.things.forEach(t => t.update());
        this.scene.things.forEach(t => t.draw(this.canvas));
    }
    scene:Scene = new Scene();
}