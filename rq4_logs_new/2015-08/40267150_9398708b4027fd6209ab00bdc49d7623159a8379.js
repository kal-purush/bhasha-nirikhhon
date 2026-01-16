(function(ns){

	var Char = function(stage,container,containerParticles) {
		this.initialize(stage,container,containerParticles);
	}
	var p = Char.prototype;
	p.stage;
	p.bitmapAnimation;
	p.container;
	p.rectChar = {};
	p.rectCollision = {};
	p.collectedShirts = 0;
	p.totalPoints = 0;
	p.posLeft;
	p.posRight;
	p.currentStatus = 'normal';
	p.currentPosition = 'center';
	p.initStage = true;
	p.hitWidth = {
		normal_center: 42,
        normal_right:30,
        normal_left:30,
        fish_center:42,
        fish_right:30,
        fish_left:30,
        invincible_center:52,
        invincible_right:43,
        invincible_left:43
	}
	p.hitHeight = {
		normal_center: 127,
        normal_right:127,
        normal_left:127,
        fish_center:127,
        fish_right:127,
        fish_left:127,
        invincible_center:132,
        invincible_right:132,
        invincible_left:132
	}
	p.particlesNormal;
	p.particlesInvencible;
	p.containerParticles;
	p.ground;
	p.startFlying = false;

	p.initialize = function(stage,container,containerParticles) 
	{
		this.stage = stage;
		this.container = container;
		this.containerParticles = containerParticles;
		this.particlesNormal = new SuperPai.Particles(this.stage,containerParticles,'#FFFFFF');
		this.particlesInvencible = new SuperPai.Particles(this.stage,containerParticles,'#ff4400');
		this.particlesNormal.pause();
		this.particlesInvencible.pause();
		this.posLeft = SuperPai.Main.STAGE_WIDTH/3;
		this.posRight = this.posLeft*2;
		//this.particlesNormal.pause();
		//
	}
	p.setupSprites = function(images)
	{
		var self = this;
		var spriteSheet = new SpriteSheet({
		    // image to use
		    images: [images], 
		    // width, height & registration point of each sprite
		    frames: {width: 70, height: 146, regX: 35, regY: 73}, 
		    animations: {
		    	stand:[5],
		    	intro: {
		    		frames: [5,4,3,2,1],
		    		next: "charging",
					frequency: 5
				},
				charging:1,
				fly: {
		    		frames: [1,2,3,4,5],
		    		next: "normal_center",
					frequency: 4
				},
		        normal_center: [6],
		        normal_right:[7],
		        normal_left:[8],
		        fish_center: {
		    		frames: [6,9,6,9],
					frequency: 6
				},
		        fish_right:{
		    		frames: [7,10,7,10],
		    		frequency: 6
				},
		        fish_left:{
		    		frames: [8,11,8,11],
		    		frequency: 6
				},
		        invincible_center:[12,14,'invincible_center',4],
		        invincible_right:[15,17,'invincible_right',4],
		        invincible_left:[18,20,'invincible_left',4]
		    }
		});
		this.bitmapAnimation = new BitmapAnimation(spriteSheet);
	
		this.bitmapAnimation.x = SuperPai.Main.STAGE_WIDTH/2;
		this.bitmapAnimation.y = SuperPai.Main.STAGE_HEIGHT - 73 - 15;
		this.bitmapAnimation.gotoAndPlay('stand');
		

		this.bitmapAnimation.onAnimationEnd = function(bitmapAnimation,name)
		{
			switch(name) 
			{
				case 'charging':
					self.particlesNormal.resume();
					if(!Modernizr.touch) SoundJS.play('startfly',SoundJS.INTERRUPT_NONE,0,0,0,0.2);
					setTimeout(function()
					{
						self.particlesNormal.type = 0;
						self.bitmapAnimation.gotoAndPlay("fly");
						Tween.get(self.bitmapAnimation).wait(200).call(function(){
							self.startFlying = true;
						}).to({y:SuperPai.Main.STAGE_HEIGHT/2},150).wait(500).call(function(){
							self.initStage = false;
							if(!Modernizr.touch) SoundJS.play('music',SoundJS.INTERRUPT_NONE,0,0,-1,1);
						});
						Tween.get(self.ground).wait(200).to({y:self.ground.y+SuperPai.Main.STAGE_HEIGHT/2},150);
					},2000);
				break;
			}
			
		}

		var graphicsGround = new Graphics();
		graphicsGround.beginFill('#4a8825').drawRect(0,SuperPai.Main.STAGE_HEIGHT-30,SuperPai.Main.STAGE_WIDTH,30);
		this.ground = new Shape(graphicsGround);
		
		this.stage.addChildAt(this.ground,0);
		this.container.addChild(this.bitmapAnimation);
	}
	p.checkCollision = function(obj)
	{
		this.rectChar.x = this.bitmapAnimation.x - this.hitWidth[this.currentStatus + '_' + this.currentPosition]/2;
		this.rectChar.y = this.bitmapAnimation.y - this.hitHeight[this.currentStatus + '_' + this.currentPosition]/2;
		this.rectChar.width = this.hitWidth[this.currentStatus + '_' + this.currentPosition];
		this.rectChar.height = this.hitHeight[this.currentStatus + '_' + this.currentPosition];
		this.rectCollision.x = obj.bitmapAnimation.x - obj.hitWidth/2;
		this.rectCollision.y = obj.bitmapAnimation.y - obj.hitHeight/2;
		this.rectCollision.width = obj.hitWidth;
		this.rectCollision.height = obj.hitHeight;

		if(this.rectOverlap(this.rectChar,this.rectCollision)){
			return true;
		}
		return false;
	}
	

	
	p.valueInRange = function(value, min, max) { return (value >= min) && (value <= max); }

	p.rectOverlap = function(rectA, rectB)
	{
	    var xOverlap = this.valueInRange(rectA.x, rectB.x, rectB.x + rectB.width) ||
	                    this.valueInRange(rectB.x, rectA.x, rectA.x + rectA.width);

	    var yOverlap = this.valueInRange(rectA.y, rectB.y, rectB.y + rectB.height) ||
	                    this.valueInRange(rectB.y, rectA.y, rectA.y + rectA.height);

	    return xOverlap && yOverlap;
	}

	p.tick = function()
	{
		if(!this.initStage && this.stage.mouseX != undefined){
			if(!SuperPai.Main.isGameOver){

				this.bitmapAnimation.x += (this.stage.mouseX - this.bitmapAnimation.x)/2;
				this.bitmapAnimation.y += (this.stage.mouseY - this.bitmapAnimation.y)/2;
			
				if(this.stage.mouseX < this.posLeft)
				{
					if(this.currentPosition != 'left')
					{
						this.currentPosition = 'left';
						this.bitmapAnimation.gotoAndPlay(this.currentStatus + '_left');
					}
					
				}
				else if(this.stage.mouseX > this.posLeft && this.stage.mouseX < this.posRight)
				{
					if(this.currentPosition != 'center')
					{
						this.currentPosition = 'center';
						this.bitmapAnimation.gotoAndPlay(this.currentStatus + '_center');
					}
				}
				else
				{
					if(this.currentPosition != 'right')
					{
						this.currentPosition = 'right';
						this.bitmapAnimation.gotoAndPlay(this.currentStatus + '_right');
					}
				}
			}
			//this.particlesNormal.tick(this.bitmapAnimation.x,this.bitmapAnimation.y);
			this.particlesInvencible.tick(this.bitmapAnimation.x,this.bitmapAnimation.y);
		}
		this.particlesNormal.tick(this.bitmapAnimation.x,this.bitmapAnimation.y);
		
	}
	p.powerMagnet = function()
	{
		this.particlesInvencible.pause();
		this.particlesNormal.resume();
		this.currentStatus = 'fish';
		this.bitmapAnimation.gotoAndPlay(this.currentStatus + '_' + this.currentPosition);
	}
	p.powerOff = function()
	{
		this.particlesInvencible.pause();
		this.particlesNormal.resume();
		this.currentStatus = 'normal';
		this.bitmapAnimation.gotoAndPlay(this.currentStatus + '_' + this.currentPosition);
	}
	p.powerInvincible = function()
	{
		this.particlesNormal.pause();
		this.particlesInvencible.resume();
		this.currentStatus = 'invincible';
		this.bitmapAnimation.gotoAndPlay(this.currentStatus + '_' + this.currentPosition);	
	}
	p.intro = function(){
		this.bitmapAnimation.gotoAndPlay("intro");
	}
	ns.Char = Char;

}(SuperPai || (SuperPai = {})));
var SuperPai;