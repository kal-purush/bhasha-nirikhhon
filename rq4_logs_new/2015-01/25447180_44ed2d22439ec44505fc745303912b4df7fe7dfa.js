/*global $, jQuery, document, window, navigator, GMaps*/
/* ==========================================================================
Document Ready Function
========================================================================== */
jQuery(document).ready(function () {


    'use strict';

    var onMobile, formInput, map, sformInput;


    $(function ($) {

        $(document).ready(function () {

            $('#switcher').stop().delay(1500).animate({ left: ($('#switcher').css('left') === '0px' ? '-' + $('#switcher').outerWidth() + 'px' : '0px')}, 1500);

            $('#switcher .open-close').click(function (e) {

                e.preventDefault();
                $('#switcher').stop().animate({ left: ($('#switcher').css('left') === '0px' ? '-' + $('#switcher').outerWidth() + 'px' : '0px')});

            });

        });

    });


    /* ==========================================================================
    Modify Copied Text
    ========================================================================== */
    function addLink() {
        var body_element, selection, pagelink, copytext, newdiv;
        body_element = document.getElementsByTagName('body')[0];
        selection = window.getSelection();
        pagelink = " Read more at: <a href='" + document.location.href + "'>" + document.location.href + "</a>";
        copytext = selection + pagelink;
        newdiv = document.createElement('div');
        newdiv.style.position = 'absolute';
        newdiv.style.left = '-99999px';
        body_element.appendChild(newdiv);
        newdiv.innerHTML = copytext;
        selection.selectAllChildren(newdiv);
        window.setTimeout(function () {
            body_element.removeChild(newdiv);
        }, 0);
    }
    document.oncopy = addLink;


    /* ==========================================================================
    Supersized Slider  ( New from v2.0 )
    ========================================================================== */
    jQuery(function ($) {
        $('#home-section-wrapper').supersized({
            slide_interval : 5000, // Length between transitions
            transition : 1, // 0-None, 1-Fade, 2-Slide Top, 3-Slide Right, 4-Slide Bottom, 5-Slide Left, 6-Carousel Right, 7-Carousel Left
            transition_speed : 900, // Speed of transition
            slide_links : '0', // Individual links for each slide (Options: false, 'num', 'name', 'blank')
            slides : [
                {image : 'images/slider/1-1920x1200.jpg', title : '', thumb : '', url : ''},
                {image : 'images/slider/2-1920x1200.jpg', title : '', thumb : '', url : ''},
                {image : 'images/slider/3-1920x1200.jpg', title : '', thumb : '', url : ''}
            ]
        });
    });


    /* ==========================================================================
    CountDown Timer
    ========================================================================== */
    $('#countdown_dashboard').countDown({
        targetDate: {
            'day': 31,
            'month': 1,
            'year': 2015,
            'hour': 8,
            'min': 30,
            'sec': 0
        },
        omitWeeks: true
    });


    /* ==========================================================================
    ScrollTo
    ========================================================================== */
    $('a.scrollto').click(function (event) {
        $('html, body').scrollTo(this.hash, this.hash, {gap: {y: -70}, animation:  {easing: 'easeInOutCubic', duration: 1700}});
        event.preventDefault();

        if ($('.navbar-collapse').hasClass('in')) {
			$('.navbar-collapse').removeClass('in').addClass('collapse');
		}

	});


    /* ==========================================================================
    Data Spy
    ========================================================================== */
    $('body').attr('data-spy', 'scroll').attr('data-target', '#nav-wrapper').attr('data-offset', '71');


    /* ==========================================================================
    on mobile?
    ========================================================================== */
	onMobile = false;
    if (/Android|webOS|iPhone|iPad|iPod|BlackBerry/i.test(navigator.userAgent)) { onMobile = true; }

    if (onMobile === true) {
        $("a[data-rel=tooltip]").tooltip('destroy');
        jQuery('#portfolio-section-wrapper').css("background-attachment", "scroll");
        jQuery('#clients-section-wrapper').css("background-attachment", "scroll");
    }


    /* ==========================================================================
    Parallax
    ========================================================================== */
    jQuery('#portfolio-section-wrapper').parallax("50%", -0.3);
    jQuery('#clients-section-wrapper').parallax("50%", -0.3);


    /* ==========================================================================
    Portfolio
    ========================================================================== */
    $(".owl-portfolio").owlCarousel({
        items : 3,
        itemsDesktop : [1000, 2],
        itemsDesktopSmall : [900, 2],
        itemsTablet: [568, 1],
        lazyLoad: true,
        autoPlay: false,
        stopOnHover: false
    });

    /* Anchors
    ---------------------------------------------------------------------------*/
    $(".portfolio-containt").mouseenter(function () {
        $(this).find('.portfolio-overlayer a.link').addClass('uk-animation-slide-left');
    });
    $(".portfolio-containt").mouseenter(function () {
        $(this).find('.portfolio-overlayer a.preview').addClass('uk-animation-slide-right');
    });
    $(".portfolio-containt").mouseleave(function () {
        $(this).find('.portfolio-overlayer a.link').removeClass('uk-animation-slide-left');
    });
    $(".portfolio-containt").mouseleave(function () {
        $(this).find('.portfolio-overlayer a.preview').removeClass('uk-animation-slide-right');
    });


    /* ==========================================================================
    Fancy Box
    ========================================================================== */
    $(".fancybox").fancybox({
        helpers : {
            overlay : {
                speedOut : 0,
                locked: false
            }
        }
    });

    $(".fancybox-media").fancybox({
        helpers : {
            media : {},
            overlay : {
                speedOut : 0,
                locked: false
            }
        }
    });


    /* ==========================================================================
    Responsive Video
    ========================================================================== */
    $('.fitvids').fitVids();


    /* ==========================================================================
    ToolTip
    ========================================================================== */
    $("a[data-rel=tooltip]").tooltip({container: 'body'});


    /* ==========================================================================
    Clients
    ========================================================================== */
    $(".owl-clients").owlCarousel({
        items : 1,
        lazyLoad: true,
        autoPlay: 5000,
        stopOnHover: false
    });


    /* ==========================================================================
    Quote Rotator
    ========================================================================== */
    $('#cbp-qtrotator').cbpQTRotator();


    /* ==========================================================================
    FORM Validation
    ========================================================================== */
    $('form#form').submit(function () {
        $('form#form .error').remove();
        $('form#form .success').remove();
        var hasError = false;
        $('.requiredField').each(function () {
            if (jQuery.trim($(this).val()) === '') {
                $(this).parent().append('<span class="error"><i class="fa fa-exclamation-triangle"></i></span>');
                hasError = true;
            } else if ($(this).hasClass('email')) {
                var emailReg = /^([\w-\.]+@([\w]+\.)+[\w]{2,4})?$/;
                if (!emailReg.test(jQuery.trim($(this).val()))) {
                    $(this).parent().append('<span class="error"><i class="fa fa-exclamation-triangle"></i></span>');
                    hasError = true;
                }
            }
        });
        if (!hasError) {
            formInput = $(this).serialize();
            $.post($(this).attr('action'), formInput, function (data) {
                $('form#form').append('<div class="success"><div class="col-md-12"><div class="alert alert-nesto"><button type="button" class="close" data-dismiss="alert" aria-hidden="true">&times;</button><p>Thanks! Your email was successfully sent. We will contact you asap.</p></div></div></div>');
            });
            $('.requiredField').val('');
        }
        return false;
    });
    $('form#form input').focus(function () {
        $('form#form .error').remove();
        $('form#form .success').remove();
    });
    $('form#form textarea').focus(function () {
        $('form#form .error').remove();
        $('form#form .success').remove();
    });


    /* ==========================================================================
    Map
    ========================================================================== */
    map = new GMaps({
        el: '#map',
        scrollwheel: false,
        lat: 29.983775,
        lng: 31.167161
    });
    map.addMarker({
        lat: 29.983775,
        lng: 31.167161,
        icon: "images/marker.png"
    });


    /* ==========================================================================
    Subscribe
    ========================================================================== */
    $('form#sform').submit(function () {
        $('form#sform .serror').remove();
        $('form#sform .ssuccess').remove();
        var shasError = false;
        $('.srequiredField').each(function () {
            if (jQuery.trim($(this).val()) === '') {
                $(this).parent().append('<span class="serror"><i class="fa fa-exclamation-triangle"></i></span>');
                shasError = true;
            } else if ($(this).hasClass('email')) {
                var emailReg = /^([\w-\.]+@([\w]+\.)+[\w]{2,4})?$/;
                if (!emailReg.test(jQuery.trim($(this).val()))) {
                    $(this).parent().append('<span class="serror"><i class="fa fa-exclamation-triangle"></i></span>');
                    shasError = true;
                }
            }
        });
        if (!shasError) {
            sformInput = $(this).serialize();
            $.post($(this).attr('action'), sformInput, function (data) {
                $('form#sform').append('<span class="ssuccess"><i class="fa fa-check"></i></span>');
            });
            $('.srequiredField').val('');
        }
        return false;
    });
    $('form#sform input').focus(function () {
        $('form#sform .serror').remove();
        $('form#sform .ssuccess').remove();
    });



}); // JavaScript Document




/* ==========================================================================
Window Scroll
========================================================================== */
jQuery(window).scroll(function () {

    'use strict';

    if (jQuery(document).scrollTop() >= 100) {
        jQuery('#nav-wrapper').addClass('tinyheader');
    } else {
        jQuery('#nav-wrapper').removeClass('tinyheader');
    }

});