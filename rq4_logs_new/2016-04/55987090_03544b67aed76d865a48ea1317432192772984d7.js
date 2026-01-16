var __extends = (this && this.__extends) || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
};
/**
 * Created by 48089748z on 21/04/16.
 */
var MyGame;
(function (MyGame) {
    var Bullet = (function (_super) {
        __extends(Bullet, _super);
        function Bullet(game, key) {
            _super.call(this, game, 0, 0, key, 0);
            this.game = game;
            this.explosion = new MyGame.Explosion(this.game);
            this.anchor.setTo(0.5, 0.5);
            this.scale.setTo(0.5, 0.5);
            this.checkWorldBounds = true;
            this.events.onOutOfBounds.add(this.killBullet, this);
            this.alive = false;
        }
        Bullet.prototype.killBullet = function (bullet) { bullet.kill(); };
        return Bullet;
    })(Phaser.Sprite);
    MyGame.Bullet = Bullet;
})(MyGame || (MyGame = {}));
//# sourceMappingURL=Bullet.js.map