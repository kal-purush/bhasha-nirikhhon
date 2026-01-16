$(document).ready(function() {

	/* window scroll */
	// $fn.scrollSpeed(step, speed, easing);
	jQuery.scrollSpeed(100, 600);

});

/* Game */

/* game state */
var GameState = {
	/* initiate game settings */
	init : function() {
		/* enable fullscreen mode */
		this.scale.fullScreenScaleMode = Phaser.ScaleManager.SHOW_ALL;
		this.scale.startFullScreen(false);
		/* center game on screen */
		this.scale.pageAlignVertically = true;
		this.scale.pageAlignHorizontally = true;
		/* start physics engine */
		this.game.physics.startSystem(Phaser.Physics.ARCADE);
		this.game.physics.arcade.gravity.y = 0;
		/* acrivate keyboard */
		this.cursor = this.game.input.keyboard.createCursorKeys();
		this.game.world.setBounds(0, 0, 575, 575);
		this.RUNNIGN_SPEED = 10;
		this.JUMPING_SPEED = 350;
		//this.ACCLERATION = 600;
		//this.DRAG = 400;
		//this.MAXSPEED = 400;
	},
	/* load the game assets before the game starts */
	preload : function() {
		/* background */
		this.load.image('background', 'assets/images/background-game.png');
		this.load.audio('theme', ['assets/audio/MoozE_Predator.mp3']);
		/* player */
		this.load.image('player', 'assets/images/su37.png');
		/* enemy */
		this.load.image('enemy', 'assets/images/su25.png');
		/* missile */
		this.load.image('missile', 'assets/images/missile.png');
	},
	/* executed after everything is loaded */
	create : function() {
		/* background */
		this.background = this.game.add.tileSprite(0, 0, 595, 595, 'background');
		this.game.physics.arcade.enable(this.background);
		this.background.body.allowGravity = false;
		this.background.body.inmovable = true;
		/* player */
		this.player = this.game.add.sprite(this.world.centerX, 550, 'player', 3);
		this.player.anchor.setTo(0.5);
		this.player.scale.setTo(0.25);
		this.game.physics.arcade.enable(this.player);
		this.player.body.gravity.y = 1000;
		this.player.body.collideWorldBounds = true;
		this.player.body.allowGravity = true;
		/* enemy */
		this.enemy = this.game.add.sprite(this.world.centerX, 50, 'enemy');
		this.enemy.anchor.setTo(0.5, 0.5);
		this.enemy.scale.setTo(0.25);
		this.game.physics.arcade.enable(this.enemy);
		this.enemy.body.allowGravity = false;
		/* missile */
	    this.missile = game.add.weapon(1, 'missile');
	    this.missile.bulletKillType = Phaser.Weapon.KILL_WORLD_BOUNDS;
	    this.missile.bulletAngleOffset = 90;
	    this.missile.bulletSpeed = 500;
	    this.missile.fireRate = 60;
	    this.missile.bulletSpeedVariance = 200;
	    this.missile.trackSprite(this.player, 0, 0);
		/* ui */
		var text = game.add.text(32, 32, 'Score: 0', {
			font : "18px Dosis",
			fill : "#9fbf00",
			align : "center"
		});
		/* camera */
		this.game.camera.follow(this.player);
		/* audio */
		music = game.add.audio('theme');
		music.volume = 0.5;
    	music.play();
	},
	/* this is executed multiple times per second */
	update : function() {
		/* animating background */
		this.background.tilePosition.y += 2;
		//this.background.tilePosition.x -= 0.5;
		/* player controls */
		if (game.input.keyboard.isDown(Phaser.Keyboard.LEFT)) {
			this.player.x -= this.RUNNIGN_SPEED;
		} else if (game.input.keyboard.isDown(Phaser.Keyboard.RIGHT)) {
			this.player.x += this.RUNNIGN_SPEED;
		}
		/*if ( game.input.keyboard.isDown( Phaser.Keyboard.UP ) ) { //&& this.player.body.touching.down
		 this.player.y -= 4;
		 } else if ( game.input.keyboard.isDown( Phaser.Keyboard.DOWN ) ) {
		 this.player.y += 4;
		 }*/
		if (game.input.keyboard.isDown(Phaser.Keyboard.SPACEBAR)) {
			//this.player.body.velocity.y = -this.JUMPING_SPEED;
			this.missile.fire();
		}
	},
	render : function render() {
		//game.debug.spriteInfo(this.player, 32, 100);
		//game.debug.inputInfo(32, 200);
		//game.debug.cameraInfo(game.camera, 32, 300);
		//game.debug.soundInfo(music, 32, 400);
	}
}

/* initiate the Phaser framework */
var game = new Phaser.Game(595, 595, Phaser.AUTO);

/* game */
game.state.add('GameState', GameState);
game.state.start('GameState');