// post on AMT
(function($, Helper) {

var $mainForm = $('#main-form'); 

$(document).ready(function() {

    // form submit
    $('#checkout').off().on('click', function(evt) {
        $(this).off();   // prevent double-clicking
        $(this).addClass('checkout-saving');
        start_form_submit(evt);
    });

    var turkSubmitTo = Helper.get_all_ids().turkSubmitTo;
    if (!Helper.is_empty(turkSubmitTo)) {
        var submitTo = turkSubmitTo + "/mturk/externalSubmit";
        $mainForm.attr('action', submitTo);
    }

});


function start_form_submit(evt) {
    var results = Helper.track_record.getCompleteResult() || {};
    update_form_info(results);

    // if debug is true, pause submit and print dataToServer
    if (Helper.track_record.debug) {
        console.log(Helper.track_record.dataToServer);
    } else {
        initiate_checkout(Helper.track_record.url, Helper.track_record.dataToServer);
    }
    evt.preventDefault();
}

function update_form_info(results) {
    $mainForm.find("input[name=meta-data]").val(JSON.stringify(results.metaData));
    $mainForm.find("input[name=all-result]").val(JSON.stringify(results.dataToAMT));
}

function initiate_checkout(url, data){
    var on_success = function(data) {
        try {

        }
        finally {
            $mainForm.submit();
        }
    };

    var on_error = function(data) {
        try {

        }
        finally {
            $mainForm.submit();
        }
    };

    Helper.initiate_ajax_handler(url, data, on_success, on_error);
}


})(jQuery, Helper);