/*Random Spawn Begin*/

var unicorns = [document.getElementById("first_uni"), document.getElementById("second_uni"), document.getElementById("third_uni"), document.getElementById("fourth_uni"), document.getElementById("fifth_uni")];


unicorns.forEach(function(unicorn){
    var x = Math.max(0, Math.min(60, Math.ceil(Math.random() * 100)));
    var y = Math.max(0, Math.min(60, Math.ceil(Math.random() * 100)));
   /*single unicorn here =>*/ $(unicorn).css({
        position: 'absolute',
        top: y + '%',
        left: x + '%',
    });
});

/*Random Spawn End*/



/*Draggable Unicorn Function Begin*/

(function($) {
    $.fn.drags = function(opt) {

        opt = $.extend({handle:"",cursor:"move"}, opt);

        if(opt.handle === "") {
            var $el = this;
        } else {
            var $el = this.find(opt.handle);
        }

        return $el.css('cursor', opt.cursor).on("mousedown", function(e) {
            if(opt.handle === "") {
                var $drag = $(this).addClass('draggable');
            } else {
                var $drag = $(this).addClass('active-handle').parent().addClass('draggable');
            }
            var z_idx = $drag.css('z-index'),
                drg_h = $drag.outerHeight(),
                drg_w = $drag.outerWidth(),
                pos_y = $drag.offset().top + drg_h - e.pageY,
                pos_x = $drag.offset().left + drg_w - e.pageX;
            $drag.css('z-index', 2).parents().on("mousemove", function(e) {
                $('.draggable').offset({
                    top:e.pageY + pos_y - drg_h,
                    left:e.pageX + pos_x - drg_w
                }).on("mouseup", function() {
                    $(this).removeClass('draggable').css('z-index', z_idx);
                });
            });
            e.preventDefault(); // disable selection
        }).on("mouseup", function() {
            if(opt.handle === "") {
                $(this).removeClass('draggable');
            } else {
                $(this).removeClass('active-handle').parent().removeClass('draggable');
            }
        });

    }
})(jQuery);


$('.unicorn').drags();

/*Draggable Unicorn Function End*/


