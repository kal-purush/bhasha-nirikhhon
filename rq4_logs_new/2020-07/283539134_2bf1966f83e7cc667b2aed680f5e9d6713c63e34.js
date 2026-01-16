$(document).ready(function() {

    "use strict";

    HideShowHeader();
    HeroSection()
    BackToTop();

});


//Owl Carousel
$('#hero-slider').owlCarousel({
    items: 1,
    loop: true,
    nav: true,
    navText: [
        '<i class="ion-ios-arrow-thin-left"></i>',
        '<i class="ion-ios-arrow-thin-right"></i>'
    ],
    navSpeed: 1000,
    autoplay: true,
    autoplayTimeout: 5000,
    autoplayHoverPause: true,
});

$('#portfolio-slider').owlCarousel({
    center: true,
    items: 1,
    loop: true,
    margin: 30,
    nav: true,
    navText: [
        '<i class="ion-ios-arrow-thin-left"></i>',
        '<i class="ion-ios-arrow-thin-right"></i>'
    ],
    navSpeed: 700,
    autoplay: true,
    autoplayTimeout: 5000,
    autoplayHoverPause: true,
    responsive : {
        // breakpoint from 480 up
        1024 : {
            items: 2,
        },
    },
});

$('#portfolio-slider2').owlCarousel({
    items: 1,
    loop: true,
    margin: 30,
    nav: true,
    navText: [
        '<i class="ion-ios-arrow-thin-left"></i>',
        '<i class="ion-ios-arrow-thin-right"></i>'
    ],
    navSpeed: 2000,
    autoplay: true,
    autoplayTimeout: 5000,
    autoplayHoverPause: true,
});

$('#portfolio-slider3').owlCarousel({
    items: 1,
    loop: true,
    margin: 30,
    nav: true,
    navText: [
        '<i class="ion-ios-arrow-thin-left"></i>',
        '<i class="ion-ios-arrow-thin-right"></i>'
    ],
    navSpeed: 2000,
    autoplay: true,
    autoplayTimeout: 5000,
    autoplayHoverPause: true,
    responsive : {
        // breakpoint from 480 up
        768 : {
            items: 2,
        },
        1024 : {
            items: 3,
        },
    },
});

$('#portfolio-slider4').owlCarousel({
    items: 1,
    loop: true,
    margin: 0,
    nav: true,
    navText: [
        '<i class="ion-ios-arrow-thin-left"></i>',
        '<i class="ion-ios-arrow-thin-right"></i>'
    ],
    navSpeed: 2000,
    autoplay: true,
    autoplayTimeout: 5000,
    autoplayHoverPause: true,
    responsive : {
        // breakpoint from 480 up
        768 : {
            items: 2,
        },
        1024 : {
            items: 4,
        },
    },
});

$('#liontestimonial').owlCarousel({
    nav: true,
    navText: [
        '<i class="ion-ios-arrow-thin-left"></i>',
        '<i class="ion-ios-arrow-thin-right"></i>'
    ],
    items: 1,
    navSpeed: 400,
    loop: true,
    autoplay: true,
    autoplayTimeout: 4000,
    autoplayHoverPause: true,
});

//Flexnav Menu
$(".flexnav").flexNav({ 'animationSpeed': 0 });

$(window).on('load', function() {
    //Portfolio masonry
    var $container = $('#portfolio-container');
    $container.isotope({
        masonry: {
            columnWidth: '.portfolio-item'
        },
        itemSelector: '.portfolio-item'
    });
    $('#filters').on('click', 'li', function() {
        $('#filters li').removeClass('active');
        $(this).addClass('active');
        var filterValue = $(this).attr('data-filter');
        $container.isotope({ filter: filterValue });
    });
});

$('.menu-button').on('click', function() {
    $('.menu-button').toggleClass('menu-open menu-close');
});

if(screen.width<1024) {
    $('.menubar .flexnav').removeClass('flexnav-show');
}


//Hide Show Header on Scroll
function HideShowHeader() {

    var didScroll;
    var lastScrollTop = 0;
    var delta = 50;
    var navbarHeight = 75;
    var navbarHideAfter = navbarHeight

    $(window).scroll(function(event) {
        didScroll = true;
    });

    if ($('.scroll-hide').length > 0) {

        setInterval(function() {
            if (didScroll) {
                hasScrolled();
                didScroll = false;
            }
        }, 100);

    }

    return false;

    function hasScrolled() {
        var st = $(this).scrollTop();

        if (Math.abs(lastScrollTop - st) <= delta)
            return;

        if (st > lastScrollTop && st > navbarHideAfter) {
            if ($('.scroll-hide').length > 0) {
                $('header').addClass('hide');
            }
        } else {
            if ($('.scroll-hide').length > 0) {
                if (st + $(window).height() < $(document).height()) {
                    $('header').removeClass('hide');
                    $('#header.transparent').addClass('white-bg');
                }
            }

            if ($(window).scrollTop() < 300) {
                $('#header.transparent').removeClass('white-bg');
            }
        }

        lastScrollTop = st;

    }

}

//Hero Section
function HeroSection() {

    var Hero = document.getElementById('hero');
    var windowScrolled;


    window.addEventListener('scroll', function windowScroll() {
        windowScrolled = window.pageYOffset || document.documentElement.scrollTop;

        Hero.style.opacity = (1 - (windowScrolled / 20) / 20);

    });

}

//Magnific Pop Up
$('.gallery').each(function() { // the containers for all your galleries
    $(this).magnificPopup({
        delegate: 'a', // the selector for gallery item
        type: 'image',
        gallery: {
            enabled: true
        }
    });
});

//Back To Top
function BackToTop() {

    $('.scrolltotop').on('click', function() {
        $('html, body').animate({ scrollTop: 0 }, 800);
        return false;
    });

    $(document).scroll(function() {
        var y = $(this).scrollTop();
        if (y > 600) {
            $('.scrolltotop').fadeIn();
        } else {
            $('.scrolltotop').fadeOut();
        }
    });

}

$('.view-map').on('click', function() {

    if (location.pathname.replace(/^\//, '') == this.pathname.replace(/^\//, '') && location.hostname == this.hostname) {
        var target = $(this.hash);
        target = target.length ? target : $('[name=' + this.hash.slice(1) + ']');
        if (target.length) {
            $('html,body').animate({
                scrollTop: target.offset().top
            }, 700);
            return false;
        }
    }
})
