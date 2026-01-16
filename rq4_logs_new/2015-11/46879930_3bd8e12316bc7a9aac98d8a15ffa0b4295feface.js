import { Sprite } from 'pixi.js';
import Utils from '../utils/number-utils';
class Nuage extends Sprite {
    constructor(options){
        var texture = PIXI.Texture.fromImage("../img/nuage.png");
        super(texture);
        this.options = options;
        this.x = this.options.x/2;
        this.y = this.options.y/4;
        this.scale.factor = 1;
        this.scale.x = this.scale.factor;
        this.scale.y = this.scale.factor;
        this.anchor.x = 0.5;
        this.anchor.y = 0.5;
        this.life = Math.random() * 2000;
        this.angle =  Utils.toRadians( Math.floor(Math.random() * 360) );
        this.v = Math.random();
        this.click = function(mouseData){
           console.log("MOUSE OVER nuage!");
        }
     }
     update(dt){
        this.angle =  Utils.toRadians( Math.random() * 360 );
        if(this.v < this.x && this.v < this.y){
            this.v = Utils.getRandom(10, 20);
        } else {
            this.v = this.y;
        }
        this.x = this.x + this.v * Math.cos(this.angle);
        this.y = this.y + this.v * Math.sin(this.angle);
        
        // if(this.life <= 1000){
        //     this.alpha =(this.life/1000);
        // }
        if(this.life <= 0){
            this.isDead =  true;
            console.log("DEAD");
        }
        // this.life -= dt;
    }
    reset(){
        this.x = this.options.x;
        this.y = this.options.y;
        this.life = Math.random()*2000;
        this.isDead = false;
    }
}
export default Nuage