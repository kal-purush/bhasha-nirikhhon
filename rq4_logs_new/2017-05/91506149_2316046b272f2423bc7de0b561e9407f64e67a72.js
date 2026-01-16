
jQuery(function() {

    var headerElement = jQuery('.header:first');
    headerElement.css('position','fixed');
    var quote = jQuery('div[sectionid="quote"]');

    jQuery('div[sectionid="quote"] .column').css('float','left');
    headerElement.css('float','left');

    var header = jQuery('div[sectionid="header"]');
    var headerLogo = jQuery('.header-logo:first');


    var scrolledDown = false;
    jQuery(window).scroll(function(event){
        var st = jQuery(this).scrollTop();
        if(scrolledDown){
            if (st < 15 && st != 0){
                //console.log(st);
                jQuery('#mainContent').animate({"margin-top":"-1px"}, 500);
                headerLogo.animate({"height":"75px"}, 500);
                scrolledDown = false;

            }
        }else{
            if (st > 15){
                //console.log(st);
                headerLogo.animate({"height":"55px"}, 500);
                jQuery('#mainContent').animate({"margin-top":"-10px"}, 500);
                scrolledDown = true;
            }
        }
        lastScrollTop = st;
    });

});

function toPosition(divid){
    jQuery('html, body').animate({scrollTop:jQuery(divid).position().top - 95 }, 'slow');
}


