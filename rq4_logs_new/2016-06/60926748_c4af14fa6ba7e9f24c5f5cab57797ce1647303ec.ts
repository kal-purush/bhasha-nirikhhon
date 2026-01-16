export class Game {
  static playerSpeed = 10;
  static playerJumpHeight = 100;
  static gravity = 800;

  static playerID = window.prompt('Enter your player ID:', '-KKFbucEljzXDLEC-49X');
  static saveCooldown = 100;

  game: Phaser.Game;
  cursors: Phaser.CursorKeys;
  player: Phaser.Sprite;
  otherPlayers: Phaser.Sprite[] = [];
  platforms: Phaser.Group;

  preload() {
    this.game.stage.backgroundColor = 0x000000;
    this.game.load.image('platform', 'assets/sprites/platform.png');
    this.game.load.image('player', 'assets/sprites/player.png');
  }

  create() {
    this.cursors = this.game.input.keyboard.createCursorKeys();
    this.game.world.setBounds(0, 0, this.game.width, this.game.height);
    this.game.physics.startSystem(Phaser.Physics.ARCADE);
    
    // TODO: Create player when registering
    // this.player = this.addPlayer();
    // firebase.database().ref('players').push({
    //   x: this.player.x,
    //   y: this.player.y
    // });

    firebase.database().ref('players').on('child_added', (playerSnapshot: any) => {
      const player = this.addPlayer(playerSnapshot.val().position);
      if (playerSnapshot.key === Game.playerID) {
        this.player = player;
        this.player.anchor.set(0.5);

        this.game.physics.arcade.enable(this.player);
        this.player.body.gravity.y = Game.gravity;

        this.game.camera.follow(this.player, Phaser.Camera.FOLLOW_LOCKON);
      }
      else {
        this.otherPlayers.push(player);
        firebase.database()
          .ref(`players/${playerSnapshot.key}/position`)
          .on('child_changed', (positionSnapshot: any) => {
            (player as any)[positionSnapshot.key] = positionSnapshot.val();
          });
      }
    });
    
    this.addPlatforms();
  }

  addPlayer(options: any = {}) {
    const player = this.game.add.sprite(
      options.x || this.game.world.centerX,
      (options.y || this.game.world.centerY) - Game.playerJumpHeight,
      'player'
    );
    player.scale.set(0.2);

    return player;
  }

  addPlatforms() {
    this.platforms = this.game.add.group();
    this.platforms.enableBody = true;
    this.platforms.createMultiple(10, 'platform');

    // Create the base platform, with buffer on either side so that the player doesn't fall through
    const buffer = 16;
    this.createPlatform(-buffer, this.game.world.height - buffer, this.game.world.width + buffer);
    
    // Create a batch of platforms that start to move up the level
    // for (let i = 0; i < 9; i++) {
    //   this.createPlatform(this.game.rnd.integerInRange(0, this.game.world.width - 50), this.game.world.height - 100 - 100 * i, 50);
    // }
  }

  createPlatform(x: number, y: number, width: number) {
    const platform = this.platforms.getFirstDead();
    platform.reset(x, y);
    platform.scale.x = width;
    platform.scale.y = 16;
    platform.body.immovable = true;

    return platform;
  }

  update() {
    if (this.player) {
      this.game.physics.arcade.collide(this.player, this.platforms);
      this.checkPlayerActions();
    }
  }

  checkPlayerActions() {
    let dx = 0;
    let angle = 0;

    if (this.cursors.right.isDown) {
      dx = Math.SQRT1_2 * Game.playerSpeed;
      angle = 30;
    }
    else if (this.cursors.left.isDown) {
      dx = -Math.SQRT1_2 * Game.playerSpeed;
      angle = -30;
    }

    if (this.cursors.up.isDown && this.player.body.touching.down) {
      this.player.body.velocity.y = Game.playerJumpHeight;
    } 

    // const newX = this.player.x + dx + (dx > 0 ? 1 : -1) * this.player.width * 0.5;
    // if (0 <= newX && newX <= this.game.width) {
    this.player.x += dx;
    // }
    
    this.player.angle = angle;

    // Set bounds as the player moves
    // http://codepen.io/jackrugile/pen/fqHtn
    if (dx) {
      this.game.world.setBounds(dx, 0, this.game.world.width + dx, this.game.height);
      this.savePlayerPosition();
    }
  }

  savePlayerPosition = _.throttle(() => {
    firebase.database()
      .ref(`players/${Game.playerID}/position`)
      .set({
        x: this.player.x,
        y: this.player.y
      });
  }, Game.saveCooldown);

  render() {
    this.game.debug.cameraInfo(this.game.camera, 32, 32);
  }
}