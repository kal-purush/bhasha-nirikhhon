class Pipe{
    constructor(height, width){
      this.width = 45;
      this.topPipe = (Math.random() * height/3 + (height/5)) ;
      this.bottomPipe = Math.random(height/2);
      this.speed = 5;
      this.start = width;
    }
    
    position(){
       var pos = [this.start, this.topPipe, this.bottomPipe, this.width, this.speed];
       return pos;
    }

    update(){
        this.speed += 5;
    }

}