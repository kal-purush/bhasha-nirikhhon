class Ball{
    constructor(x,y,width,height){
        this.body = Matter.Bodies.circle(x,y,radious,{isStatic:true})
        World.add(world,this.body);
        this.width = width;
        this.height = height;
    }
    display(){
        fill("red");
    }
}