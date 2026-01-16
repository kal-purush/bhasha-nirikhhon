"use strict";
var auxFunc = {
    calcMove: function(obj){
        if(obj.directionX === null || obj.directionY === null){
            var b = obj.destX - obj.x;
            var c = obj.destY - obj.y;
            var a = Math.sqrt( (b*b) + (c*c) );
            var temp = a / obj.speed;
            obj.directionX = b / temp;
            obj.directionY = c / temp;
        } else {
            obj.x += obj.directionX;
            obj.y += obj.directionY;
        };
        if(((obj.type === "ghost") || (obj.type === "hunter")) && (arsenal.currentLevel >= 2)){
            if(auxFunc.rollDice(0.5)){
                obj.destX = auxFunc.randomXPosition();
                obj.directionX = null;
            }
        }

        if(obj.acceleration){
            if(obj.target.hp > 0){  
                obj.follow(obj.target);
            } else {
                obj.stopAndRetarget();
            }  
        }
    },
    drawObject: function(ctx, obj){     //ctx => canvas 2d
        obj.draw()
    },
    randomColor: function(){
        var color = "#";
        for(var i = 0 ; i < 6 ; i++){
            var dice = parseInt(Math.floor(Math.random() * (16)));
            dice -= 1;
            if(dice < 0){dice = 0};
            if(dice > 9){
                switch(dice){
                    case 10:
                        dice = "A";
                        break;
                    case 11:
                        dice = "B";
                        break;
                    case 12:
                        dice = "C";
                        break;
                    case 13:
                        dice = "D";
                        break;
                    case 14:
                        dice = "E";
                        break;
                    case 15:
                        dice = "F";
                        break;
                    default:
                        dice = 0;
                }
            };
            color += dice;
        };
        return color;
        
    },
    randomXPosition: function(){
        var temp = Math.floor(Math.random() * (780)) + 10;
        if(arsenal.existant.length == 1){
            while((arsenal.existant[0].x > temp-32) && (arsenal.existant[0].x < temp+32)){
                temp = Math.floor(Math.random() * (780)) + 10;
            }
        }
        if(arsenal.existant.length >= 2){
            var xPositions = [];
            var spacesAvailable = [];
            var spacefound = false;
            for(var i = 0; arsenal.existant[i]; i++){
                xPositions[i] = arsenal.existant[i].x;
            };
            xPositions.sort(function(a, b) {
                return a - b;
              });
            // console.log(xPositions);
            for(var i = 0; xPositions[i]; i++){
                if((i == 0) && (xPositions[0] > 58) && (!spacesAvailable[0])){
                    spacesAvailable.push([10,xPositions[0]-16]);
                } else if(!xPositions[i+1] && (xPositions[i] < 742)) {
                    spacesAvailable.push([xPositions[i]+16,790]);
                } else {
                    if((xPositions[i+1] - xPositions[i]) > 64 ){
                        spacesAvailable.push([xPositions[i]+16,xPositions[i+1]-16]);
                    }
                }
            };
            while(spacefound == false){
                for(var i = 0; spacesAvailable[i]; i++){
                    if(temp-16 > spacesAvailable[i][0] && temp+16 < spacesAvailable[i][1]){
                        // console.log("spaces available: ", spacesAvailable);
                        // console.log("X chosen: "+ temp);
                        return temp;
                    }
                }
                temp = Math.floor(Math.random() * (780)) + 10;
            };
        }
        return temp;
    },
    findTarget: function(){
        for(var i = arsenal.player.ship.y ; i>0 ; i-=2){
            for (var j = 0; arsenal.existant[j] ; j++){
                if((arsenal.existant[j].y > i) && (arsenal.existant[j].targetedBy === undefined) ){
                    // console.log(arsenal.existant[j].x, arsenal.existant[j].y)
                    arsenal.existant[j].targetedBy = this;
                    return arsenal.existant[j]
                } else {
                    // weapons.missile.stopAndRetarget();
                };
            };
        };
    },
    findAngle: function(obj){
        var b = obj.destX - obj.x;
        var c = obj.destY - obj.y;
        var a = Math.hypot(c,b);
        obj.angle = Math.acos(b/a) * 360 / (Math.PI * 2);
        if(obj.y > obj.destY){
            obj.angle = -(obj.angle);
        }
    },
    rollDice: function(percentage){
        var dice = Math.random()*100;
        if(dice <= percentage){
            return true;
        } else {
            return false;
        }
    },
    gameOver: function(){
        window.cancelAnimationFrame(arsenal.stopScene);
            ctx.clearRect(0,0, 800, 600);
            ctx.fillStyle = 'black';
            ctx.font = '42px arial';
            ctx.fillText("Vous avez perdu !", 200, 300);
            ctx.fillText(("Votre Score: " + arsenal.player.score), 200, 350);
    }
}