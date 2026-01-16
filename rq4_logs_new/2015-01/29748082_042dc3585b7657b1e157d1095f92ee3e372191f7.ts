export class InfoScreen extends Phaser.State {
    text: Phaser.Text;
    raw: string;
    constructor(text: string) {
        super();
        this.raw = text;
    }
    create() {
        this.text = new Phaser.Text(this.game, 32, 32, this.raw, {font: "35px Arial", fill: "#333"});
        this.text.alpha = 0;
        this.game.add.existing(this.text);
        this.game.add.tween(this.text).to({alpha: 1}, 500).start();
        this.input.keyboard.addKey(Phaser.Keyboard.SPACEBAR).onDown.addOnce(() => {
            this.game.state.start('MainMenu');
        }, this);
    }
}