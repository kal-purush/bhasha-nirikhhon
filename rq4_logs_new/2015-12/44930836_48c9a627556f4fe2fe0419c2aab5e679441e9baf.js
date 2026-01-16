$(window).scroll(function(){
    var height = $(window).scrollTop();
    if (height > 100){
        if ( $("nav").hasClass("drop") == false ){
            $("nav").addClass("drop")
        }
    }
    else
    {
        if ( $("nav").hasClass("drop") == true ){
            $("nav").removeClass("drop")
        }
    }
});