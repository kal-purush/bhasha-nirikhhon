var VictoryScene = {
    create: function () {
        console.log("Victory");
        var button = this.game.add.button(400, 300,
            'button',this.actionOnClick, this, 2, 1, 0);

        button.anchor.set(0.5);

        var goText = this.game.add.text(400, 100, "¡Enhorabuena!");

        var text = this.game.add.text(0, 0, "Restart");
        text.anchor.set(0.5);
        goText.anchor.set(0.5);
        button.addChild(text);
        
        
        var button2 = this.game.add.button(400, 150,
                                          'button',
                                          this.actionOnClick2, 
                                          this, 2, 1, 0);
        button2.anchor.set(0.5);
        var text2 = this.game.add.text(0, 0, "Return Menu");
        text2.anchor.set(0.5);
        button2.addChild(text2);
    },
    
    
    actionOnClick: function(){
        this.game.state.start('preloader');
    },

   
    actionOnClick2: function(){
        
        this.game.state.start('menu');
    }
};

module.exports = VictoryScene;