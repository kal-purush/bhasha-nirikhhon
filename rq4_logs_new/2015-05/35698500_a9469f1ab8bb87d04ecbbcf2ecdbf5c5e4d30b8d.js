

var tpj=jQuery;
tpj.noConflict();
var revapi1;

tpj(document).ready(function() {

    if(tpj('#rev_slider_1_1').revolution == undefined)
        revslider_showDoubleJqueryError('#rev_slider_1_1');
    else
        revapi1 = tpj('#rev_slider_1_1').show().revolution(
            {
                dottedOverlay:"none",
                delay:5000,
                startwidth:2000,
                startheight:800,
                hideThumbs:200,

                thumbWidth:100,
                thumbHeight:50,
                thumbAmount:5,

                navigationType:"bullet",
                navigationArrows:"none",
                navigationStyle:"round",

                touchenabled:"on",
                onHoverStop:"on",

                navigationHAlign:"center",
                navigationVAlign:"bottom",
                navigationHOffset:0,
                navigationVOffset:50,

                soloArrowLeftHalign:"left",
                soloArrowLeftValign:"center",
                soloArrowLeftHOffset:20,
                soloArrowLeftVOffset:0,

                soloArrowRightHalign:"right",
                soloArrowRightValign:"center",
                soloArrowRightHOffset:20,
                soloArrowRightVOffset:0,

                shadow:0,
                fullWidth:"on",
                fullScreen:"off",

                stopLoop:"off",
                stopAfterLoops:-1,
                stopAtSlide:-1,


                shuffle:"off",

                autoHeight:"on",
                forceFullWidth:"on",


                hideThumbsOnMobile:"off",
                hideBulletsOnMobile:"off",
                hideArrowsOnMobile:"off",
                hideThumbsUnderResolution:0,

                hideSliderAtLimit:0,
                hideCaptionAtLimit:0,
                hideAllCaptionAtLilmit:0,
                startWithSlide:0,
                videoJsPath:"revslider/rs-plugin/videojs/",
                fullScreenOffsetContainer: "#header"
            });

});	//ready

jQuery(function(){

    var $container = jQuery('#portitems4');

    $container.isotope({
        itemSelector : '.item4'
        ,filter : '.ultimos'
    });

    var $relTag = jQuery('.home-portfolio-categories a');

    $relTag.removeAttr('href');

    var $optionSets = jQuery('#remove'),
        $optionLinks = $optionSets.find('a');

    $optionLinks.click(function(){
        var $this = jQuery(this);
        // don't proceed if already selected
        if ( $this.hasClass('selected') ) {
            return false;
        }
        var $optionSet = $this.parents('#remove');
        $optionSet.find('.selected').removeClass('selected');
        $this.addClass('selected');

        // make option object dynamically, i.e. { filter: '.my-filter-class' }
        var options = {},
            key = $optionSet.attr('data-option-key'),
            value = $this.attr('data-option-value');
        // parse 'false' as false boolean
        value = value === 'false' ? false : value;
        options[ key ] = value;
        if ( key === 'layoutMode' && typeof changeLayoutMode === 'function' ) {
            // changes in layout modes need extra logic
            changeLayoutMode( $this, options )
        } else {
            // otherwise, apply new options
            $container.isotope( options );
        }

        return false;
    });


});
jQuery(document).ready(function(){
    // Slider
    var $slider = jQuery(".slides-testimonials").bxSlider({
        controls: true,
        displaySlideQty: 1,
        default: 6000,
        pause: 10000,
        speed:1000,
        auto:true,
        easing : "ease-in-out",
        prevText : "<i class='icon-chevron-left icon-1x'></i> ",
        nextText : "<i class='icon-chevron-right icon-1x'></i> ",
        pager :false
    });
});

jQuery(document).ready(function(){jQuery("a[rel^='lightbox']").prettyPhoto({theme:'light_rounded',show_title: true, deeplinking:false,callback:function(){scroll_menu()}});  });

jQuery(document).ready(function(){
    jQuery('.searchform #s').val('Search...');

    jQuery('.searchform #s').focus(function() {
        jQuery('.searchform #s').val('');
    });

    jQuery('.searchform #s').focusout(function() {
        jQuery('.searchform #s').val('Search...');
    });

});