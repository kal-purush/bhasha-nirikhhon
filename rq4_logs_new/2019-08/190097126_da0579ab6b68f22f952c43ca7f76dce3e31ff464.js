$(document).ready(function() {
    $("#content-slider").lightSlider({
        loop: true,
        keyPress: true,
        item: 4,
        slideMargin: 20,
        auto: true,
        loop: true,
        autoWidth: false,
        responsive: [{
                breakpoint: 800,
                settings: {
                    item: 1,
                    slideMove: 1,
                }
            },
            {
                breakpoint: 480,
                settings: {
                    item: 1,
                    slideMove: 1
                }
            }
        ]
    });
});