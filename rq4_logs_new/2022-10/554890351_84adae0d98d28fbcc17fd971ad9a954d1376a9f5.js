$('.gnb').click(function(){
    $('.tab_gnb_inner').animate({left:0},700,'swing');
    $('.dark').css({'display':'block'});
});

$('.tab_gnb_content_close').click(function(){
    $('.tab_gnb_inner').animate({left:-750},700,'swing');
    $('.dark').css({'display':'none'});
});


$('.gnb').click(function(){
    $('.mb_gnb_inner').animate({left:0},700,'swing');
    $('.dark').css({'display':'block'});
});

$('.mb_gnb_content_close').click(function(){
    $('.mb_gnb_inner').animate({left:-350},700,'swing');
    $('.dark').css({'display':'none'});
});

$('.dark').click(function(){
    $('.tab_gnb_inner').animate({left:-750},700,'swing');
    $('.mb_gnb_inner').animate({left:-350},700,'swing');
    $('.dark').css({'display':'none'});
    });

$(window).resize(function(){
    if( $(window).width() > 768){
        $('.tab_gnb_inner').removeAttr('style');
    }
})