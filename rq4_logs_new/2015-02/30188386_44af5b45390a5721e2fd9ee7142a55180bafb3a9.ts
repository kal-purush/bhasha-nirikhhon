module WebSketch {
    //  ブートステート
    export class Boot extends Phaser.State {

        preload() {
            this.load.removeAll();
            this.load.image("white", "assets/white64_64.png");

            //  ステージ別コンテンツはここでは一気に読み込む仕組みにしているがレベル型ステージの場合は個々で読み込ませるべきだろう
            Practice1.loadContents(this.game);
        }

        create() {
            this.stage.disableVisibilityChange = true;

            this.scale.scaleMode = Phaser.ScaleManager.RESIZE;
            this.scale.fullScreenScaleMode = Phaser.ScaleManager.RESIZE;

            this.game.state.start("Practice1", true, false);
        }
    }
} 