

    function loadMonster(mobSpeed, mobX, mobY, mobDirection) {

        //  imgMonsterARun.onload = handleImageLoad;
        //  imgMonsterARun.onerror = handleImageError;
        imgMonsterARun.src = "assets/images/MonsterARun.png";

        // create spritesheet and assign the associated data.
        var spriteSheet = new createjs.SpriteSheet({
            // image to use
            images: [imgMonsterARun],
            // width, height & registration point of each sprite
            frames: { width: 64, height: 64, count: 10, regX: 32, regY: 32 },
            animations: {
                walk: [0, 9, "walk"]
            }
        });

        // create a BitmapAnimation instance to display and play back the sprite sheet:
        bmpAnimation = new createjs.Sprite(spriteSheet);

        // start playing the first sequence:
        bmpAnimation.gotoAndPlay("walk");     //animate

        // set up a shadow. Note that shadows are ridiculously expensive. You could display hundreds
        // of animated rats if you disabled the shadow.
        bmpAnimation.shadow = new createjs.Shadow("#454", 0, 5, 4);
        bmpAnimation.name = "monster1";
        bmpAnimation.direction = 900;

        //Speed
        bmpAnimation.vX = mobSpeed;
        //X Plane (Starting pos)
        bmpAnimation.x = mobX;
        //Y Plane (Run on this line)
        bmpAnimation.y = mobY;
        //Left or Right
        bmpAnimation.scaleX = mobDirection;
        //Start at frame one
        bmpAnimation.currentFrame = 0;

        stage.addChild(bmpAnimation);
        stage.update();
    }

    function newMonster() {

       //Set monster speed
        var speed = Math.floor(Math.random() * (8 - 1 + 1)) + 1;
        var y = Math.floor(Math.random() * ((window.innerHeight - 16) - 1 + 1)) + 1;
       loadMonster(speed, 16, y, 90);
    }


/*
        bmpAnimation.direction = 90;
        bmpAnimation.vX = 4;
        bmpAnimation.x = 16;
        bmpAnimation.y = 32;
 */