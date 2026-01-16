jQuery(document).ready(function(t) {
    "use strict";

    t('.owl-ca-carousel').addClass('owl-carousel'); // So css works

    t(".carousel-anything-container").each(function() {
        t(this).owlCarousel({
            autoplay : "false" === t(this).attr("data-autoplay") ? !1 : t(this).attr("data-autoplay"),
            loop     : true,
            rewind   : false,
            items    : t(this).attr("data-items"),
            responsive : {
                0:{
                    items: t(this).attr("data-items-mobile")
                },
                479:{
                    items: t(this).attr("data-items-tablet")
                },
                768:{
                    items: t(this).attr("data-items")
                }
            },
            touchDrag: "false" === t(this).attr("data-touchdrag") ? !0 : !1,
            mouseDrag: "false" === t(this).attr("data-touchdrag") ? !0 : !1,
            autoplayHoverPause: "true" === t(this).attr("data-stop-on-hover") ? !0 : !1,
        });
    });
});