function zoomer(x, y, xveloc, yveloc, width, height, effect){
    this.x = x;
    this.y = y;
    this.xveloc = xveloc;
    this.yveloc = yveloc; 
    this.width = width;
    this.height = height;
    this.effect = effect;
    this.loop = function(){
        this.updatePos();
        for(var i = 0; i < camLen; i++){
            cameras[i].drawRect(this.x, this.y, this.width, this.height, "blue", false); //Box frame
            cameras[i].drawLine(this.x+this.width/10, this.y+this.height/2, this.x+(9/10)*this.width, this.y+this.height/2, "blue"); //Arrow Body
            cameras[i].drawLine(this.x+(2/3)*this.width, this.y+(1/4)*this.height, this.x+(9/10)*this.width, this.y+this.height/2, "blue"); //Arrow Head
            cameras[i].drawLine(this.x+(2/3)*this.width, this.y+(3/4)*this.height, this.x+(9/10)*this.width, this.y+this.height/2, "blue"); //Arrow Head
        }
    }
    this.updatePos = function(){
        this.groundCeilBounce();
        this.x += this.xveloc;
        this.y += this.yveloc;
    }
    this.groundCeilBounce = function(){
        if(this.y+this.height >= gameArea.groundLevel || this.y <= gameArea.ceilLevel){
            this.yveloc *= -1;
        }
    }
}
function nature(x, y, width, height, type){
    this.x = x;
    this.y = y;
    this.width = width;
    this.height = height;
    this.type = type;
    this.loop = function(){ //Yeah unecessary but this is my standard
        this.draw();
    }
    this.draw = function(){
        for(var i = 0; i < plaLen; i++){
            if(this.type == "tree" && players[i].nature[0] == "on"){ //Checking if tree nature tree (true). ALSO WHY CAN'T IT JUST BE A TRUE OR FALSE DANGIT HTML
                cameras[i].drawRect(this.x, this.y, this.width, this.height, "brown", true);
                for(var q = 0; q < 4; q++){
                    cameras[i].drawCirc(this.x+10*(Math.random()-0.5), this.y+10*(Math.random()-0.5), Math.random()*20+5, "green"); //The trees just blow in the wind really really really fast don't worry about it
                }
            }
            if(this.type == "cloud" && players[i].nature[1] == "on"){
                cameras[i].drawRect(this.x, this.y, this.width, this.height, "white", true); //I really really really don't feel like making proper clouds for this. Please don't make me. (I might do it if I'm bored some time, in that case ignore this comment. Implemented as white rectangles.)
            }
            if(this.type == "bush" && players[i].nature[2] == "on"){
                cameras[i].drawRect(this.x, this.y, this.width, this.height, "green", true); //I really really really don't feel like making proper bushes for this. Please don't make me. (I might do it if I'm bored some time, in that case ignore this comment. Implemented as green rectangles.)
            }
        }
    }
}