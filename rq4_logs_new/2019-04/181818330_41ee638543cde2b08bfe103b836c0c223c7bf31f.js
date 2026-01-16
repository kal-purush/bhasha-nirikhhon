
$(document).ready(function(){
    // -----------------------------home_carousel------------------//
    var $home_carousel = $('.home_carousel'),
        $home_carousel_ul = $('.home_carousel_ul'),
        $li = $home_carousel_ul.children('li').eq(0).clone(true),
        $btn = $('.home_carousel_btn>li'),
        $prev = $('.home_carousel_prev'),
        $next = $('.home_carousel_next'),
        $l = 0;
    $home_carousel_ul.append($li);
    $home_carousel_ul.width($home_carousel_ul.width() + $li.width());
    $home_carousel_ul.children('li').css('width', '400px');
    //获取轮播li的数量
    var $liNum = $home_carousel_ul.children('li').length;
    var $timer = setInterval(scroll, 1000);
    function scroll() {
        $l -= $li.width();
        if ($l < -($liNum-1) * $li.width()){
            $l = -$li.width();
            $home_carousel_ul.css('left', 0)
        }
        $home_carousel_ul.stop().animate({left: $l}, 300);
        if ($l / -$li.width() == ($liNum-1)){
            $btn.eq(0).stop().fadeTo(300,0.8).siblings('li').stop().fadeTo(300,0.3)
        }else{
            $btn.eq($l / -$li.width()).stop().fadeTo(300,0.8).siblings('li').stop().fadeTo(300,0.3)
        }
    }
    $home_carousel.hover(function(){
        clearInterval($timer);
        $('.home_carousel_prev,.home_carousel_next').stop().fadeIn(300)
    },function(){
        $timer = setInterval(scroll, 1000);
        $('.home_carousel_prev,.home_carousel_next').stop().fadeOut(300)
    });
    $btn.eq(0).css({'opacity': 0.8});
    $btn.mouseenter(function(){
        var $index = $(this).index();
        $l = -$index * $li.width();
        $home_carousel_ul.stop().animate({left: $l});
        $(this).stop().fadeTo(300,0.8).siblings('li').stop().fadeTo(300,0.3);
    });
    $prev.click(function(){
        $l += $li.width();
        if ($l / -$li.width() < 0){
            $home_carousel_ul.css({ 'left': -($liNum-1) * $li.width() });
            $l = -($liNum-2) * $li.width();
        }
        $home_carousel_ul.stop().animate({ 'left': $l });
        $btn.eq($l/-$li.width()).stop().fadeTo(300,0.8).siblings('li').stop().fadeTo(300,0.3)
        content();
    });
    $next.click(function(){
        $l -= $li.width();
        if ($l / -$li.width() > ($liNum-1)){
            $home_carousel_ul.css({ 'left': 0 });
            $l = -$li.width();
        }
        $home_carousel_ul.stop().animate({ 'left': $l });
        if ($l / -$li.width() == $liNum-1){
            $btn.eq(0).stop().fadeTo(300,0.8).siblings('li').stop().fadeTo(300,0.3)
        }else{
            $btn.eq($l/-$li.width()).stop().fadeTo(300,0.8).siblings('li').stop().fadeTo(300,0.3)
        }
        content();
    });
    // 防止双击文本被选中
    function content(){
        $(this).mousedown(function(){
            $(this).mouseup(function(){
                return false;
            });
            return false;
        })
    }





});