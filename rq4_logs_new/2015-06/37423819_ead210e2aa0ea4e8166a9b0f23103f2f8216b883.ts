///<reference path="Tank.ts"/>
///<reference path="MyMath.ts"/>

class CPUTankController{
    constructor(public tank : Tank){

    }

    update(delta:number){
        this.tankCPUMove(this.tank.leader.tankBody.position, delta);
        this.tankCPURotate(this.tank.leader.tankBody.position);
        this.tankCPUAimTurret(this.tank.leader);
    }

    tankCPUMove = (target:Phaser.Point, delta:number) => {
        var angleToTarget = Math.atan2(target.y - this.tank.tankBody.y, target.x - this.tank.tankBody.x)*180/Math.PI;
        var myAngle = this.tank.tankBody.angle;

        var offset:number = angleToTarget - myAngle;
        if(offset <= -180) offset += 360;
        if(offset > 180) offset -= 360;

        var scale = ((90 - Math.abs(offset))/180)/0.5;

        //if(this.tankBody.position.distance(target) <= this.disToFollow)
        //    this.currSpeed = this.leader.currSpeed;

        var timeToStop = Math.abs(this.tank.currSpeed/(this.tank.acceleration*this.tank.brakeMultiplier*delta));
        var dis = target.distance(this.tank.tankBody.position);
        console.log("dis: "+dis+", timeToStop: "+timeToStop+", currSpeed: "+this.tank.currSpeed+", accel+mult: "+(this.tank.acceleration*this.tank.brakeMultiplier));
        if(timeToStop*this.tank.currSpeed < dis) {
            //If we are stopping, give us a good bonus for brakes
            if (this.tank.currSpeed > 0 && scale < 0) {
                this.tank.currSpeed += this.tank.acceleration*this.tank.brakeMultiplier*scale;
                //Otherwise, accelerate normally.
            } else
                this.tank.currSpeed += this.tank.acceleration * scale;
        }else{
            console.log("Stopping");
            if(this.tank.currSpeed > 0) this.tank.currSpeed -= this.tank.acceleration*this.tank.brakeMultiplier;
            else this.tank.currSpeed += this.tank.acceleration*this.tank.brakeMultiplier;
        }

        //console.log("angleToTarget: "+angleToTarget+", myAngle: "+myAngle+", offset: "+offset+", scale: "+scale+" currSpeed: "+this.currSpeed);
    };

    tankCPUAimTurret = (target:Tank) => {
        var angle = Math.atan2(target.tankBody.y - this.tank.tankBody.y, target.tankBody.x - this.tank.tankBody.x)*180/Math.PI;
        //console.log("Angle: "+angle);
        this.tank.turret.angle = MyMath.lerp(this.tank.turret.angle, angle, this.tank.turretRotationSpeed);
    };

    tankCPURotate = (target:Phaser.Point) => {
        var angleToTarget = Math.atan2(target.y - this.tank.tankBody.y, target.x - this.tank.tankBody.x)*180/Math.PI;
        var myAngle = this.tank.tankBody.angle;

        var offset = angleToTarget - myAngle;
        if (offset <= -180) offset += 360;
        if (offset > 180) offset -= 360;

        //Reverse the tank if we are closer to that.
        //if(offset < -90 || offset > 90)
            //angleToTarget = MyMath.normalize(angleToTarget+180);

        this.tank.tankBody.angle = MyMath.lerp(myAngle, angleToTarget, this.tank.rotationSpeed);
    };
}