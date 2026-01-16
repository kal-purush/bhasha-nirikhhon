define(['jquery', 'underscore', 'lib/selectFx'], function ($, _) {

    function init(container) {
        (function () {
            [].slice.call(document.querySelectorAll('select.cs-select')).forEach(function (el) {
                new SelectFx(el);
            });
        })();
    }
    return {
        init: function (container) {
            init(container);
        }
    };
});