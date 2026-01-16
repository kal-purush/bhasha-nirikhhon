(function(global) {

    var Board = {

        bricks : {},

        create : function() {

            bricksGroup = game.add.group();
            bricksGroup.x = Config.board.x;
            bricksGroup.y = Config.board.y;

            for (var row = 0; row < Config.board.rows; row++) {

                bricks[row] = {};

                for (var col = 0; col < Config.board.cols; col++) {
                    var brick = createbrick(row, col);
                }
            }

            gameObject.add(bricksGroup);
            this.createTimeBar(); 
        },

        createTimeBar: function() {

            countDown = Config.timeBar.countDown;

            var percent = 1 / Config.timeBar.countDown * 100;
            var subTimeBar = Config.timeBar.height * percent/100;

            timeBar = game.add.tileSprite(
                Config.timeBar.x, Config.timeBar.y, Config.timeBar.width, Config.timeBar.height, 'time-bar'
            ); 

            countDownTimer = game.time.create(false);
            countDownTimer.loop(Phaser.Timer.SECOND, function() {

                countDown -= 1;

                timeBar.height -= subTimeBar;
                timeBar.y += subTimeBar;
            });

            countDownTimer.start(); 
        }

    } 


})(this);