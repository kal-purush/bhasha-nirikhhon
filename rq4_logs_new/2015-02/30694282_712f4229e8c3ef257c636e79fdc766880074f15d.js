requirejs.config({
    // baseUrl: 'js/lib',
    paths: {
        jQuery: "https://ajax.googleapis.com/ajax/libs/jquery/2.1.3/jquery.min"
    },
    shim: {
        jQuery: {
            exports: 'jQuery'
        }
    }
});

requirejs(
    ['jQuery', '../js/VerticalSlideShow'], function (jQuery, VerticalSlideShow) {

    jQuery(document).ready( function () {
        document.body.scrollTop = document.documentElement.scrollTop = 0;
        new VerticalSlideShow({
            container: '#container',
            selector: 'img'
        });
    });
});
