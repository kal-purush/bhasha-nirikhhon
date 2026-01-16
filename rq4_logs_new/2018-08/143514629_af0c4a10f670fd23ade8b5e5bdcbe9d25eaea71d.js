BossCreatividad=function(game,bullets,player){	
	

	this.game=game;
	this.bullets=bullets;
	this.player=player;
	Phaser.Sprite.call(this,game,game.world.centerX+300,game.world.centerY+100,'bossCreatividad')
	this.scale.setTo(-0.5,0.5);
	this.anchor.setTo(0.5);
	game.physics.arcade.enable(this);

	this.body.allowGravity=false;
	shootingTime=4000;
	shootingElapsed=shootingTime;
	game.add.existing(this);
  	}

BossCreatividad.prototype=Object.create(Phaser.Sprite.prototype);

BossCreatividad.prototype.constructor=BossCreatividad;
BossCreatividad.prototype.shoot=function(){
	bullet=this.bullets.getFirstDead();
	if(bullet){
		bullet.reset();
	}
	else{
		bullet=new BulletBoss1(this.game,this.x,this.y-30,this.game.rnd.integerInRange(1,2),this.player.position.x,this.player.position.y);
		this.bullets.add(bullet);
	}
	
};
BossCreatividad.prototype.update=function(){

		shootingElapsed+=this.game.time.elapsed;

		if(shootingElapsed>=shootingTime){
					this.shoot('bala');
					shootingElapsed=0;

			}

	}