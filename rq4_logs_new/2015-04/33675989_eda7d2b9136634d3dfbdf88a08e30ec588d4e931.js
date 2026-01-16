var modeState = {
    create: function(){
        this.game.add.sprite(0,0,'cover');
        this.chanButton = this.game.add.button(this.game.world.centerX, 125, 'chanmode',this.chanMode, this ,1,0,2);
        this.chanButton.anchor.setTo(0.5,0);
        this.jiejiButton = this.game.add.button(48, 328, 'chanmode',this.chanMode, this ,4,3,5);
        this.wujinButton = this.game.add.button(281, 328, 'chanmode',this.chanMode, this ,7,6,8);

    },

    chanMode:function(){
        game.state.start('gameState');
    }
}