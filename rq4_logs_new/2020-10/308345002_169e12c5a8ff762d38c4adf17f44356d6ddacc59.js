class dustbin {
    constructor(x,y,width,height) {
        var options={
            
            
            'density' :1.0
        }
        this.body=Bodies.rectangle(x,y,width,height,options);
        this.width=width;
        this.height=height;
        World.add(world,this.body);


    }
    display() {
        push();
        translate(this.body.position.x,this.body.position.y);
        rotate(this.body.angle);
       
        rectMode(CENTER);
        strokeWeight(4);
        stroke("red");
        fill("red");
        rect(0,0,this.width,this.height)
        pop();
    }
}