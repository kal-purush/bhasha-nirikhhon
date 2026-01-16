$(document).ready(function () {
    //text fade-in
    $('.text-block').animate({marginLeft:"0"},1500)
    
    var scrollLink = $('.scroll');
    
        //smooth scrolling
    
        scrollLink.click(function(e){
            e.preventDefault();
            $('body, html').animate({
                scrollTop: $(this.hash).offset().top
            }, 1000);
        });
        
    
        //active link switching
        $(window).scroll(function(){
            var scrollbarLocation = $(this).scrollTop();
            
            scrollLink.each(function () {
                var sectionOffset = $(this.hash).offset().top
    
                if (sectionOffset <= scrollbarLocation && sectionOffset <= scrollbarLocation ) {
                    $('nav ul li a').removeClass("active");
                    $(this).addClass('active');
                }
    
            })
        })

        $('header').click(function() {
            $('nav ul').show(400);
        })
});