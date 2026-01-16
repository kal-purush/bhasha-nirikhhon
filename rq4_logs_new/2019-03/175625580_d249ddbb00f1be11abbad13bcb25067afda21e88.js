$(document).ready(function () {
    //tooltips init
    $('[data-toggle="tooltip"]').tooltip({
        container:  'body'
    });

    //hide-show sidebar
    var main = $('#main'),
        sidebar = $('#sidebar'),
        f_btn = $('#full_screen');

    f_btn.on('click', function () {
        if (f_btn.hasClass('active')) {
            main.removeClass('col-md-12').addClass('col-md-8 col-md-offset-4 col-lg-9 col-lg-offset-3');
            sidebar.addClass('col-md-4 col-lg-3').removeClass('hidden');
        } else {
            main.removeClass('col-md-8 col-md-offset-4 col-lg-9 col-lg-offset-3').addClass('col-md-12');
            sidebar.removeClass('col-md-4 col-lg-3').addClass('hidden');
        }
    });

    //drag rows on table
    $('.dragtable').tableDnD({
        onDragClass: 'drag'
    });


    //just example. Modal window close, alert init
    // (login page, remind password)
    var remind = $('#remind');
    if (remind.length > 0) {
        $('#passw_sent_init').on('click', remind, function () {
            remind.modal('hide');
            $('#passw_sent').removeClass('hidden');
        });
    }
});