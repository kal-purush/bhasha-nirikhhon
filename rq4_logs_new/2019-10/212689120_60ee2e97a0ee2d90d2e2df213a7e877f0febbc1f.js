$(document).ready(function(e) {
    // Toggle Active Class on Hero Items

    $('.hero-item').click(function() {
        if(!$(this).hasClass('active')) {
            $('.hero-item.active').removeClass('active');
            $(this).addClass('active');
        }

        // Background Colors on Hero Items
        if($('.hero-item:nth-of-type(3)').hasClass('active')) {
            $('.hero-item:nth-of-type(2) .color-overlay').css('background-color', 'rgba(3, 23, 62, .67)');
        } else if ($('.hero-item:nth-of-type(1)').hasClass('active')) {
            $('.hero-item:nth-of-type(2) .color-overlay').css('background-color', 'rgba(224, 88, 42, .67)');
        } else {
            $('.hero-item:nth-of-type(2) .color-overlay').css('background-color', 'transparent');
        }
    });
});