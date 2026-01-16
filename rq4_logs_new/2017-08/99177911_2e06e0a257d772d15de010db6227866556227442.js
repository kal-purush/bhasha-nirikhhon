var modalDiv = '<div class="modal fade" id="ajaxModal" tabindex="-1" role="dialog"><div class="modal-dialog" role="document"></div></div>';

// Load the widgets
window.lazyload = function(){
    var wt = $(window).scrollTop();    //* top of the window
    var wb = wt + $(window).height();  //* bottom of the window

    $(".ajax-widget").each(function(i, widget){
        var ot = $(this).offset().top;  //* top of object (i.e. advertising div)
        var ob = ot + $(this).height(); //* bottom of object

        if (!$(this).attr("loaded") && $(this).is(':visible') && wt <= ob && wb >= ot) {
            var $widget = $(widget),
                url     = $widget.data('url');

            $widget.attr("loaded", true);

            $.ajax({
                type: 'GET',
                url: url,
                dataType: 'html',
                success: function (data) {
                    $widget.html(data);
                }
            });
        }
    });
};

$(document).ready(function() {
    var buttons = $('button[type="submit"]', 'form.ajax').data('loading-text', " <i class='fa fa-spinner fa-spin'></i> loading");

    $(document).on('click', 'a.popup', function(e) {
        e.preventDefault();

        var modal = $('#ajaxModal'),
            link  = $(this),
            size  = link.data('modal-size');

        if (modal.length == 0) {
            $(modalDiv).appendTo('body');
            modal = $('#ajaxModal');
        }

        var dialog = $('.modal-dialog', modal);

        dialog.removeClass('modal-lg modal-sm');
        if (size)
            dialog.addClass('modal-' + size);

        $.ajax({
            type: 'GET',
            url: $(this).attr('href'),
            dataType: 'html',
            success: function (data) {
                dialog.html(data);
            },
            complete: function () {
                modal.modal();
            }
        }).fail(function (jqXhr, json, errorThrown) {
            if (jqXhr.status == 401)
                noty({ type: 'error', text: 'You must be logged in to perform this action' });
            // Let's deal with this sucker another time
        });
    });

    $(document).on('submit', 'form.ajax', function(event) {
        var form     = $(this),
            formData = form.serializeArray(),
            action   = form.attr('action'),
            callback = form.data('callback'),
            onLoad   = form.data('onload'),
            submit   = $('input[type="submit"], button[type="submit"]', form),
            modal    = form.closest('.modal');

        if (form.hasClass('invalid')) return false;

        // submit.attr('disabled', 'disabled');
        submit.button('loading');
        if (onLoad) {
            var fn = window[onLoad];

            if (typeof(fn) === 'function')
                fn();
        }

        // process the form
        $.ajax({
            type: 'POST',
            url: action,
            data: formData,
            dataType: 'json'
        })
        .done(function (data) {
            if (data.failure) {
                noty({ type: 'error', text: data.failure });

                if (modal.length)
                    modal.modal('hide');

                return false;
            }

            if (data.error !== undefined) {
                $.each(data.error, function (index, value) {
                    var input = $('input#' + index + ', select#' + index + ', textarea#' + index, form),
                        group = input.closest('.input-group'),
                        error = group.length ? group.next('small.error') : input.next('small.error'),
                        block = input.closest('.form-group');

                    // Special case for Google ReCaptcha
                    if (index == 'g-recaptcha-response') {
                        group = $('.input-group.recaptcha', form);
                        error = group.next('small.error');
                        block = group.closest('.form-group');
                    }

                    error.html(value);

                    block.addClass('has-error');
                });

                if ($('.recaptcha', form).length)
                    grecaptcha.reset();

                return false;
            }

            if (data.redirect !== undefined) {
                setTimeout(function(){
                        document.location.href = data.redirect.location
                    },
                    data.redirect.timeout ? data.redirect.timeout : 10);

                return false;
            }

            if (data.reload !== undefined) {
                document.location.reload(true);

                return false;
            }

            if (data.replace !== undefined) {
                for (var key in data.replace)
                    $(key).html(data.replace[key]);

                return false;
            }

            if (callback) {
                var fn = window[callback];

                if (typeof(fn) === 'function')
                    fn(form, data.callbackData);
            }

            if (typeof data.success === 'string') {
                noty({ type: 'success', text: data.success });

                if (!form.hasClass('no-close') && modal.length)
                    modal.modal('hide');
            }
        })
        .fail(function (jqXhr, json, errorThrown) {
            if (jqXhr.status == 401)
                noty({ type: 'error', text: 'You must be logged in to perform this action' });

            var errors = jqXhr.responseJSON;

            if (errors) {
                $.each(errors, function (index, value) {
                    var input = $('input#' + index + ', select#' + index + ', textarea#' + index, form),
                        group = input.closest('.input-group'),
                        error = group.length ? group.next('small.error') : input.next('small.error'),
                        block = input.closest('.form-group');

                    // Special case for Google ReCaptcha
                    if (index == 'g-recaptcha-response') {
                        group = $('.input-group.recaptcha', form);
                        error = group.next('small.error');
                        block = group.closest('.form-group');
                    }

                    error.html(value);

                    block.addClass('has-error');
                });

                if ($('.recaptcha').length)
                    grecaptcha.reset();
            }
        })
        .always(function () {
            submit.button('reset');
        });

        // stop the form from submitting the normal way and refreshing the page
        event.preventDefault();
    });

    $(document).on('click', '.ajax-widget a', function(e){
        if ($(this).hasClass('external'))
            return;

        e.preventDefault();
        var container = $(this).closest('.ajax-widget');

        $.ajax({
            type: 'GET',
            url: $(this).attr('href'),
            dataType: 'html',
            success: function (data) {
                container.html(data);
            }
        });
    });

    $(document).on('click', 'a.ajax', function(e){
        e.preventDefault();

        var callback = $(this).data('callback');

        $.ajax({
            type: 'GET',
            url: $(this).attr('href'),
            dataType: 'json'
        })
        .done(function (data) {
            if (data.failure) {
                noty({ type: 'error', text: data.failure });

                return false;
            }

            if (data.redirect !== undefined) {
                setTimeout(function(){
                        document.location.href = data.redirect.location
                    },
                    data.redirect.timeout ? data.redirect.timeout : 10);

                return false;
            }

            if (data.reload !== undefined) {
                document.location.reload(true);

                return false;
            }

            if (data.replace !== undefined) {
                for (var key in data.replace)
                    $(key).replaceWith(data.replace[key]);

                return false;
            }

            if (callback) {
                var fn = window[callback];

                if (typeof(fn) === 'function')
                    fn(data.callbackData);
            }

            if (typeof data.success === 'string') {
                noty({ type: 'success', text: data.success });
            }
        })
        .fail(function (jqXhr, json, errorThrown) {
            if (jqXhr.status == 401)
                noty({ type: 'error', text: 'You must be logged in to perform this action' });
            else noty({ type: 'error', text: 'Unknown Error' });
        });
    });

    $(document).on('click', '.modal .btn-submit', function(e){
        $('form.ajax', '.modal').trigger('submit');
    });

    $('.modal').on('hidden.bs.modal', function (e) {
        var modal = $('.modal');

        $('form', modal).each(function(index, form){
            $('.has-error', form).removeClass('has-error');
            form.reset();
        });
    });

    $(window).scroll(lazyload);
    lazyload();
});