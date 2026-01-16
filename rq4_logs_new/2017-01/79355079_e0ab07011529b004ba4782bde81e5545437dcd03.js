var winHeight;
var winWidth;
var scrollTop;
var isMobile = false;
var isTablet = false;
$(document).ready(function () {
    initVariables();
    setWinHeight();
    initParalaxScroll();
    setTimeout(function () {
        $('.gif-block').addClass('hide');
        $('body').addClass('no-scroll');
        $('.gif-block img').hide();
    }, 1800);
    setTimeout(function () {
        $('.main-wrapper').addClass('show');
        $('body').removeClass('no-scroll');
    }, 2100);
    setTimeout(function () {
        $('.hero-wrapper .desc').addClass('show');
    }, 2500);
});

$(window).resize(function () {
    initVariables();
});

$(window).load(function () {
    initVariables();
});

function initVariables() {
    winHeight = $(window).height();
    winWidth = $(window).width();
    if (winWidth < 600) {
        isMobile = true;
    } else
        isMobile = false;

    if (winWidth > 600 && winWidth < 1024) {
        isTablet = true;
    } else
        isTablet = false;
}

function setWinHeight() {
    $('.win-height').each(function () {
        $(this).height($(window).height() * .75);
    });
}

function initParalaxScroll() {
    $(window).scroll(function () {
        var wScroll = $(this).scrollTop();
        $('.work-item').each(function () {
            var pageItemTop = $(this).offset().top;
            if (wScroll > pageItemTop - winHeight / 1.2) {
                $(this).addClass('show');
            }
        });
        $('.progress-list .progress-item').each(function () {
            var pageItemTop = $(this).offset().top;
            if (wScroll > pageItemTop - winHeight / 1.2) {
                var progressBarWidth = $(this).find('.progress-bar').attr('data-width');
                var progressBarColor = $(this).find('.progress-bar').attr('data-bg-color');
                $(this).find('.progress-bar-bg').css("width", progressBarWidth + '%').css("background", progressBarColor);
            }

        });

    });
}