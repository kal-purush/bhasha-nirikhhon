/* Template Name: Devazo - Landing
   Author: Themesdesign
   Version: 1.0.0
   Created: Sep 2019
   File Description: Main JS file of the template
*/

(function ($) {

    'use strict';
    
    // STICKY
    $(window).scroll(function() {
        var scroll = $(window).scrollTop();

        if (scroll >= 50) {
            $(".sticky").addClass("nav-sticky");
        } else {
            $(".sticky").removeClass("nav-sticky");
        }
    });

    // SmoothLink
    $('.navbar-nav a').on('click', function(event) {
        var $anchor = $(this);
        $('html, body').stop().animate({
            scrollTop: $($anchor.attr('href')).offset().top - 0
        }, 1500, 'easeInOutExpo');
        event.preventDefault();
    });

    // scrollspy
    $("#navbarCollapse").scrollspy({
        offset: 70
    });

})(jQuery)