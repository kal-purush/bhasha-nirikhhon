export class PlayerInfo extends Phaser.Group {
    name: string;
    bg: Phaser.Sprite;
    namerender: Phaser.Text;
    specializations: Array<number> = [];
    specializationbars: Array<Phaser.Sprite> = [];
    selected: boolean = false;
    constructor(game: Phaser.Game, type: number) {
        super(game);
        this.game.add.existing(this);
        this.x = (this.game.width/4) * type;
        this.y = this.game.height - 64;
        this.fixedToCamera = true;
        this.bg = new Phaser.Sprite(this.game, this.x, this.y, '1');
        this.bg.fixedToCamera = true;
        this.bg.scale.y = 64;
        this.bg.scale.x = (this.game.width/4);
        this.bg.tint = 0xf8f8f8;
        this.add(this.bg);
        this.game.add.existing(this.bg);
        for(var i = 0; i < 4; i++) {
            this.specializations[i] = (i == type ? 0.7 : 0.1);
        }
        for(var i = 0; i < 4; i++) {
            var bar = new Phaser.Sprite(this.game, this.x + 100, this.y + i * 8, '1');
            switch(i) {
                case 0:
                    bar.tint = 0xff0000;
                    break;
                case 1:
                    bar.tint = 0x00ff00;
                    break;
                case 2:
                    bar.tint = 0x0000ff;
                    break;
                case 3:
                    bar.tint = 0xffff00;
                    break;
            }
            bar.scale.y = 8;
            bar.scale.x = this.specializations[i] * 100;
            bar.fixedToCamera = true;
            this.specializationbars[i] = bar;
            this.add(bar);
            this.game.add.existing(bar);
        }
        switch(type) {
            case 0:
                this.name = "asd4";
                break;
            case 1:
                this.name = "asd3";
                break;
            case 2:
                this.name = "asd2";
                break;
            case 3:
                this.name = "asd1";
                break;
        }
        this.namerender = new Phaser.Text(this.game, this.x, this.y, this.name, {
            fill: "#333", font: "bold 18px Arial", align: "right"
        });
        this.namerender.fixedToCamera = true;
        this.add(this.namerender);
        this.game.add.existing(this.namerender);
    }

    addTo(n: number) {
        var amo = 0;
        if(this.specializations[n] >= 0.99) {
            return;
        }
        for(var i = 0; i < 4; i++) {
            amo += this.specializations[i] / 100;
            this.specializations[i] *= 0.99;
        }
        this.specializations[n] += amo;
        this.updatebars();
    }

    updatebars() {
        for(var i = 0; i < 4; i++) {
            this.specializationbars[i].scale.x = this.specializations[i] * 100;
        }
    }

    select() {
        this.selected = true;
        this.bg.tint = 0xcccccc;
    }

    unselect() {
        this.selected = false;
        this.bg.tint = 0xf8f8f8;
    }

}