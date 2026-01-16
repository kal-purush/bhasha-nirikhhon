$(document).ready(function() {
    jQuery.fn.rotate = function(degrees) {
        $(this).css({ 'transform': 'rotate(' + degrees + 'deg)' });
        return $(this);
    };
    $(".burger").on('click', function() {
        $(".menu").slideToggle("fast", function() {});
        $('.burger span:first-child').toggleClass("rotateMinus");
        $('.burger span:nth-child(2)').toggleClass("rotatePlus");
        $('.burger').toggleClass("drop");
        $('.burger span:last-child').toggle();
    });
    $(window).on('resize', function() {
        var win = $(this);
        if (win.width() <= 980) {
            $(".menu").removeClass('block');
        } else {
            $(".menu").addClass('block');
        }
    });
});