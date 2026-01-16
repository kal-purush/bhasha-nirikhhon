(function (window, undefined) {
    'use strict';
    var $container = $('#wrapper-container');
    $container.masonry({
        columnWidth: 200,
        itemSelector: '.post-item'
    });
} (window, undefined));