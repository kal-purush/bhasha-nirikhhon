'use strict';

var EVENTS = require('./../../common/scripts/constants/EVENTS');
var BEM = require('./../../common/scripts/constants/BLOCKS');
var ATTRS = require('./../../common/scripts/constants/ATTRS');

var block = BEM.BUTTON;

$(document).ready(function () {
    var $block = $('.' + block);

    $block.on(EVENTS.ELEMENT.CLICK, function () {
        if($(this).attr(ATTRS.DATA_OPEN.NAME) === ATTRS.DATA_OPEN.VALUES.LIGHTBOX) {
            $(document).trigger(EVENTS.CUSTOM.FORM.OPEN, {
                dataFormType: $(this).attr(ATTRS.DATA_FORM_TYPE.NAME),

                dataServiceName: $(this).attr(ATTRS.DATA_SERVICE_NAME.NAME)
            });
        }
    });
});