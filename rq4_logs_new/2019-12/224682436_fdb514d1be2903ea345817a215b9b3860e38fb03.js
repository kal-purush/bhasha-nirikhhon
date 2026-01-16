class SceneOver extends Phaser.Scene {
    constructor(){
        super('SceneIntro');
    }
    preload(){
        this.load.image('sky', 'images/sky.png');//from wuzz game
    }
    create(){
        console.log("Game Over!");
        let bg = this.add.image(0,0,"sky").setOrigin(0,0);
        Align.scaleToGameW(bg,2);

        this.aGrid = new AlignGrid({
            scene: this,
            rows: 11,
            cols: 11
        });
        //this.aGrid.showNumbers();

        var gameOverText=this.add.text(0,0,"Game Over",{color:'#fffff',fontSize:game.config.width/10});
        gameOverText.setOrigin(0.5,0.5);
        this.aGrid.placeAtIndex(27,gameOverText);
        //add some background images here. 

        var playAgainText=this.add.text(0,0,"Play Again!",{color:'#fffff',fontSize:game.config.width/10});
        playAgainText.setOrigin(0.5,0.5);
        this.aGrid.placeAtIndex(82,playAgainText);

        playAgainText.setInteractive();
        playAgainText.on("pointerdown",this.playAgain,this);
    }
    playAgain()
    {
        this.scene.start("SceneMain");
    }
    
    update(){

    }
}