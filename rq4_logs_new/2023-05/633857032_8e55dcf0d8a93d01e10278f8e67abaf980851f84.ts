import PVector from "./PVector";
import p5Types from "p5";
import staticp5 from "../../staticp5";

export default class Mover {

    location: PVector;
    velocity: PVector;
    acceleration: PVector;

    constructor (width: number, height: number) {
        this.location = new PVector(Math.floor(staticp5.random(width)),Math.floor(staticp5.random(height)));
        this.velocity = new PVector(staticp5.random(5),staticp5.random(5));
        this.acceleration = new PVector(-0.001,0.01);
        this.velocity.limit(10);
        console.log(staticp5.width)
    }

    update() {
        this.velocity.add(this.acceleration);
        this.velocity.limit(10);
        console.log(this.velocity.magnitude());
    }

    display(p5: p5Types) {
        p5.stroke(0);
        p5.fill(175);
        p5.ellipse(this.location.x, this.location.y, 10, 10);
    }

    checkEdges(p5: p5Types) {
        if (this.location.x > p5.width) {
            this.location.x = 0;
        } else if (this.location.x < 0) {
            this.location.x = p5.width;
        }
        
        if (this.location.y > p5.height) {
            this.location.y = 0;
        } else if (this.location.y < 0) {
            this.location.y = p5.height;
        }
    }
}