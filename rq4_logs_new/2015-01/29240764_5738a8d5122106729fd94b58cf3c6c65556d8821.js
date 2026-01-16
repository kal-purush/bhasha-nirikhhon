(function ($) {
    "use strict";

    var $document = $(document);

    $document.on('click', '[data-mmenu="toggle"]', function (e) {
        var $menu = $(this).closest('li.mmenu-dropdown');

        var isMobile = $menu.css('position') === 'static';
        if (!isMobile) {
            return;
        }

        e.preventDefault();
        $menu.addClass('click-open');
    });

    $document.on('click', '[data-mmenu="back"]', function (e) {
        var $menu = $(this).closest('li.mmenu-dropdown');

        var isMobile = $menu.css('position') === 'static';
        if (!isMobile) {
            return;
        }

        e.preventDefault();
        $menu.removeClass('click-open');
    });

})(jQuery);