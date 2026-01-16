
class wall {

    constructor(x, y, width, height) {

        var opt = {
        
         isStatis:false,
        
        }

        this.body = Bodies.rectangle(x, y, width, height, opt);
        this.width = width;
        this.height = height;
        World.add(world, this.body);

    }
    display() {
        var pos = this.body.position;
        var angle = this.body.angle;
        push();
        translate(pos.x, pos.y);
        rotate(angle);
        rectMode(CENTER);
        fill("red");
        rect(0, 0, this.width, this.height);
        pop();
    }


}