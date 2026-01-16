/**
 * Created by ho on 10/30/2016.
 */

function Misc(EXP, player, enemy) {
    this.EXP = EXP;
    this.player = player;
    this.enemy = enemy;
    this.ArrayOfEverything = [player];
    for (var i = 0; i < enemy.length; i++) {
        this.ArrayOfEverything.push(enemy[i]);
    }
    ;


    this.worldImage = loadImage("../images/background.png");
    this.displayWorld = function () {
        imageMode(CENTER);
        image(this.worldImage, width / 2, height / 2, width, height);
    };

    this.updateOrder = function () {
        this.ArrayOfEverything.sort(function (a, b) {
            return b.radius - a.radius;
        });
    };
    this.displayScore = function () {

        var position = createVector(width / 2, height / 2);
        var posX = position.x + 75;
        var posY = position.y;
        this.updateOrder();
        var total = 10;
        if (this.ArrayOfEverything.length < 10) {
            total = this.ArrayOfEverything.length;
        }

        fill(0, 0, 0, 128);
        //position.x
        rect(position.x, position.y, 150, total * 25, 15);
        for (var i = 0; i < total; i++) {
            textAlign(CENTER);
            fill(255);
            textSize(16);
            textLeading(5);
            text(i + 1 + ". " + this.ArrayOfEverything[i].name + ": " + Math.round(this.ArrayOfEverything[i].radius), posX, posY += 20);
        }
    };

    this.setCenter = function () {
        translate(width / 2, height / 2);
        scale(50 / this.player.radius);
        translate(-this.player.pos.x, -this.player.pos.y);
    }


}