
const config = {
    type: Phaser.AUTO,
    width: 800,
    height: 600,
    physics: {
        default: "arcade",
        arcade: { gravity: { y: 0 } }
    },
    scene: { preload, create, update }
};

const game = new Phaser.Game(config);

let player, cursors, bullets, attackKey;

function preload() {
    this.load.image("player", "player.png");
    this.load.image("bullet", "bullet.gif");
}

function create() {
    player = this.physics.add.sprite(400, 300, "player").setScale(1.5);
    player.setCollideWorldBounds(true);

    bullets = this.physics.add.group();

    cursors = this.input.keyboard.createCursorKeys();
    attackKey = this.input.keyboard.addKey(Phaser.Input.Keyboard.KeyCodes.SPACE);

    this.input.keyboard.on("keydown-W", () => shootBullet(this));
}

function update() {
    player.setVelocity(0);
    if (cursors.left.isDown) player.setVelocityX(-200);
    if (cursors.right.isDown) player.setVelocityX(200);
    if (cursors.up.isDown) player.setVelocityY(-200);
    if (cursors.down.isDown) player.setVelocityY(200);
}

function shootBullet(scene) {
    let bullet = bullets.create(player.x, player.y, "bullet");
    bullet.setVelocityX(300);
}