$(window).on('resize', function () {
    var win = $(this);
    if (win.width() > 767) {
               $('#moreOptions').addClass('dropdown-menu-right');
    }
    else {
        $('#moreOptions').removeClass('dropdown-menu-right');
    }

});
