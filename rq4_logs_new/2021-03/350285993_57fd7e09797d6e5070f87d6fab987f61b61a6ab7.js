class Game{
    constructor(canvas){
        this.objectList = [];
        let img = new Image();
        img.src = "/player.png";
        let p = new Player(220, 740, 200, 40, 60, img);
        this.objectList.push(p);

        for(let i = 0; i<50; i++){
            this.objectList.push(new Poop());
        }

        this.gameOver = false;
        this.time = 0;
        this.ctx = canvas.getContext("2d");

        setInterval(()=>{
            this.update();
            this.render(this.ctx);
        }, 1000/60);
    }

    update(){
        if(this.gmaeOver)
            return;
        this.objectList.forEach( object => object.update());

        let p = this.objectList[0];

        for(let i = 1; i< this.objectList.length; i++){
            if( p.checkCol(this.objectList[i]) ){
                this.gameOver = true;
            }
        }
        this.time += 1/60;
    }

    render(){
        if(this.gameOver)
            return;
        this.ctx.clearRect(0, 0, 500, 800);

        this.ctx.fillStyle = "#000";
        this.ctx.font = "25px Arial";
        this.ctx.fillText(Math.round(this.time*100)/100.0, 400, 50);
        this.objectList.forEach( object => object.render(this.ctx));
    }
}