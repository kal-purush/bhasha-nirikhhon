$(document).ready(function() {

    $(".toggle-mnu").click(function () {
        $(this).toggleClass("on");
        $(".main-head__list").slideToggle();
        return false;
    });
    $('.slick-slider').slick({
        slidesToShow: 3,
        slidesToScroll: 1,
        autoplay: false,
        autoplaySpeed: 2000,
        infinite: false,
        responsive: [
            {
                breakpoint: 1300,
                settings: {
                    arrows: false,
                    centerMode: false,
                    dots: true
                }
            },
            {
                breakpoint: 768,
                settings: {
                    arrows: false,
                    centerMode: false,
                    slidesToShow: 2,
                    dots: true
                }
            },
            {
                breakpoint: 480,
                settings: {
                    arrows: false,
                    centerMode: false,
                    slidesToShow: 1,
                    slidesToScroll: 1,
                    dots: true
                }
            }
        ]
    });
    $('.section-4__slider').slick({
        slidesToShow: 6,
        slidesToScroll: 1,
        autoplay: false,
        autoplaySpeed: 2000,
        infinite: false,
        responsive: [
            {
                breakpoint: 1300,
                settings: {
                    arrows: false,
                    centerMode: false,
                    dots: true
                }
            },
            {
                breakpoint: 768,
                settings: {
                    arrows: false,
                    centerMode: false,
                    slidesToShow: 4,
                    dots: true
                }
            },
            {
                breakpoint: 480,
                settings: {
                    arrows: false,
                    centerMode: false,
                    slidesToShow: 2,
                    slidesToScroll: 1,
                    dots: true
                }
            }
        ]
    });
    $('.section-5__slider').slick({
        slidesToShow: 3,
        slidesToScroll: 1,
        autoplay: false,
        autoplaySpeed: 2000,
        dots: true,
        infinite: false,
        responsive: [
            {
                breakpoint: 1300,
                settings: {
                    arrows: false,
                    centerMode: false,
                    dots: true
                }
            },
            {
                breakpoint: 768,
                settings: {
                    arrows: false,
                    centerMode: false,
                    slidesToShow: 2,
                    dots: true
                }
            },
            {
                breakpoint: 480,
                settings: {
                    arrows: false,
                    centerMode: false,
                    slidesToShow: 1,
                    slidesToScroll: 1,
                    dots: true
                }
            }
        ]
    });
    $('.s10-slider').slick({
        slidesToShow: 3,
        slidesToScroll: 1,
        autoplay: false,
        autoplaySpeed: 2000,
        infinite: false,
        responsive: [
            {
                breakpoint: 1300,
                settings: {
                    arrows: false,
                    centerMode: false,
                    dots: true
                }
            },
            {
                breakpoint: 768,
                settings: {
                    arrows: false,
                    centerMode: false,
                    slidesToShow: 2,
                    dots: true
                }
            },
            {
                breakpoint: 480,
                settings: {
                    arrows: false,
                    centerMode: false,
                    slidesToShow: 1,
                    slidesToScroll: 1,
                    dots: true
                }
            }
        ]
    });
    setTimeout(function() {
        $('.slider-wrapp').css('overflow', 'visible');
    }, 1000);

    var filtered = false;

    $('.link').on('click', function () {
        var type = '.type-' + $(this).attr('data-type');

        if (filtered === false) {
            $('.section-5__slider').slick('slickUnfilter');
            $('.section-5__slider').slick('slickFilter', type);
        } else {
            $('.section-5__slider').slick('slickUnfilter');
            $('.section-5__slider').slick('slickFilter', type);
            $('.section-5__slider').slickGoTo(0);
            filtered = false;
        }
         $('.section-5__slider img').hide().fadeIn(500);
    });




    $(".section-8").waypoint(function() {

        $('.counter').each(function () {
            $(this).prop('Counter',0).animate({
                Counter: $(this).text()
            }, {
                duration: 5000,
                easing: 'swing',
                step: function (now) {
                    $(this).text(Math.ceil(now));
                }
            });
        });

    }, {
        offset : "50%"
    });

});