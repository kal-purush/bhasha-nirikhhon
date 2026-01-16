/// <reference path="../ui/Keyboard.ts" />
/// <reference path="../utils/math/toRad.ts" />

module com.spacewarsts.display {

    import Keyboard = ui.Keyboard;
    import toRad = utils.math.toRad;

    export class Asteroids extends createjs.Shape {

        private deltaY:number;
        private deltaX:number;
        private canvasCenter;

        constructor (canvasSize) {

            super();

            this.canvasCenter= {x:canvasSize.w/2, y:canvasSize.h/2};


            //this.x = this.initX = config.xPos;
            //this.y = this.initY = config.yPos;
            //this.rotation = config.angle;
            //


            this.init();
            this.setInitialPosition();
            this.getDirection();
        }
        private setInitialPosition():void {
            var angle= this.getAngle();
           this.x= this.canvasCenter.x+ Math.cos(angle)* 200;
           this.y= this.canvasCenter.y+ Math.sin(angle)* 200;
        }

        private getAngle(){
            return toRad(Math.random()*360);
        }


        init():void {
            this.graphics.beginFill("#FFFFFF")
            .drawCircle(0,0,10)
            .endFill();
        }


        private getDirection(){
            var angleRotation= Math.atan2(this.canvasCenter.y - this.y, this.canvasCenter.x - this.x) * 180 / Math.PI;

            this.deltaX = (Math.cos(toRad(angleRotation))* 2);
            this.deltaY = (Math.sin(toRad(angleRotation))* 2);
        }

        update():void {
            this.x += this.deltaX;
            this.y += this.deltaY;
        }

    }
}