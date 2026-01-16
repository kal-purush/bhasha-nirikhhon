
window.APP = (function (module, $) {
    "use strict";

    var m = module;

    m.bookingDetailsComponent = {}; // module

    /**
      * @desc setup form datepicker
    */
    var setDatepicker = function () {
        if ($.fn.datepicker) {
            $('.booking-details__arrival-date .field--datepicker').datepicker({
                dateFormat: 'dd/mm/yy',
                minDate: '+0d',
                onClose: function (dateText, inst) {
                    $(this).attr("readonly", false);
                },
                beforeShow: function (input, inst) {
                    $(this).attr("readonly", true).blur();
                }
            });
        }

    };

    // modules to fire on pageload
    m.bookingDetailsComponent.init = function () {

        if($('.booking-details-component').length === 0) {
            return true;
        }

        setDatepicker();
    };

    return module;

})(window.APP || {}, window.jQuery);