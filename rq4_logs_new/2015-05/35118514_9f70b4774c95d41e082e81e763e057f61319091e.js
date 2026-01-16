(function($) {
    $.fn.customDropdown = function(options) {
        options = options || {};
        var dropdownTriggerSelector = this.selector;
        var blockToShowSelector = options.blockToShowSelector || 'ul';
        var blockToChangeSelector = options.blockToChangeSelector || 'span';
        var itemToPickSelector = options.itemToPickSelector || 'a';
        var activeClassToAdd = options.activeClassToAdd || 'active';
        var isSelectable = options.isSelectable !== false;

        var itemToPickFullSelector = [
            dropdownTriggerSelector,
            blockToShowSelector,
            itemToPickSelector
        ].join(' ');

        var show = function ($dropdownContainer) {
            var $blockToShow = $dropdownContainer.find(blockToShowSelector);
            setTimeout(function () {
                $dropdownContainer.addClass(activeClassToAdd);
            }, 100);
            $blockToShow.show().focus();
        };

        var hide = function ($dropdownContainer) {
            var $blockToHide = $dropdownContainer.find(blockToShowSelector);
            $blockToHide.hide(0, function () {
                $dropdownContainer.removeClass(activeClassToAdd);
            });
        };

        $(document).on('click', dropdownTriggerSelector, function (e) {
            e.preventDefault();
            var $dropdownContainer = $(this);

            if ($dropdownContainer.hasClass(activeClassToAdd)) {
                hide($dropdownContainer);
            } else {
                show($dropdownContainer);
            }
        });

        $(document).on('click', itemToPickFullSelector, function (e) {
            e.preventDefault();
            var $itemToPick = $(this),
                $dropdownContainer = $itemToPick.closest(dropdownTriggerSelector);

            setTimeout(function () {
                hide($dropdownContainer);
                if (isSelectable) {
                    var $blockToChange = $dropdownContainer.find(blockToChangeSelector);
                    $itemToPick.addClass(activeClassToAdd)
                        .siblings()
                        .removeClass(activeClassToAdd);
                    $blockToChange.text($itemToPick.text());
                }
            }, 100);
        });

        $('html').on('click', function (e) {
            if (!$(e.target).parents(dropdownTriggerSelector).size()) {
                $(dropdownTriggerSelector).each(function () {
                    var $dropdownContainer = $(this);
                    if ($dropdownContainer.hasClass(activeClassToAdd)) {
                        hide($dropdownContainer);
                    }
                });
            }
        });
    };
})(jQuery);

$(document).ready(function(){
    $('.category-select').customDropdown();
    $('.sub-pages').customDropdown({isSelectable: false});
});