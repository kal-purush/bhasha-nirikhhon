$(document).ready(function(){
    Snake = function(){
        return {
            init: function(){
                $('body').html('');
                this.buildStage()
            },
            
            buildStage: function (){
                this.stage = $('<div class="stage" />').css({
                    'height': parseInt($(window).height()/2),
                    'margin-top': parseInt($(window).height()/4)
                }).appendTo('body');
                var self = this;
                $('<div class="startBtn">Go!</div>').appendTo(this.stage).click(function(){
                    $(this).animate({
                        opacity: 0,
                        height: 0,
                        width: 0,
                        "line-height": 0,
                        "font-size": 0
                    }, 400, function(){
                        $(this).remove();
                        self.startTheGame();
                    })
                });
            },
            startTheGame: function(){
                alert('Under construction');
            }
        }
    };
    var game = new Snake();
    game.init();
});