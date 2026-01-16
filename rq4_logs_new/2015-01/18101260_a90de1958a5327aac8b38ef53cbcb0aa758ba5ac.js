;(function ($) {

    $.fn.addResourceWidget = function(params) {

        params = $.extend({
            path: null,
            onSuccess: null
        }, params);

        var $modal = $('#modal');

        this.each(function() {
            $(this).bind('click', function(e) {
                e.preventDefault();

                $.ajax({
                    url: params.path,
                    dataType: 'xml',
                    cache: false
                })
                .done(function(xmldata) {
                    /* TODO CDATA title */
                    var $title = $(xmldata).find('title');
                    var $form = $(xmldata).find('form');
                    if($title.length == 1) {
                        $modal.find('.modal-title').html($title.html());
                    }
                    if($form.length == 1) {
                        $form = $($form.text());
                        $form.find('.form-footer a.form-cancel-btn').click(function(e) {
                            e.preventDefault();
                            $modal.modal('hide');
                        });

                        $form.ajaxForm({
                            dataType: 'json',
                            success: function(data) {
                                params.onSuccess(data);
                                $modal.modal('hide');
                            }
                        });

                        $modal
                            .off('shown.bs.modal')
                            .on('shown.bs.modal', function() {
                                $form.formWidget();
                                initTinyMCE();
                            });

                        $modal.find('.modal-body').html($form);
                        $modal.modal({show:true});
                    }
                });
            });
        });

        return this;
    };

    $.fn.listResourceWidget = function(params) {

        params = $.extend({
            path: null,
            onSelection: null
        }, params);

        var $modal = $('#modal');

        this.each(function() {
            $(this).bind('click', function(e) {
                e.preventDefault();

                $.ajax({
                    url: params.path,
                    dataType: 'xml',
                    cache: false
                })
                .done(function (xmldata) {
                    /* TODO CDATA title */
                    var $title = $(xmldata).find('title');
                    var $list = $(xmldata).find('list');
                    if ($title.length == 1) {
                        $modal.find('.modal-title').html($title.html());
                    }
                    if ($list.length == 1) {
                        $list = $($list.text());
                        $modal
                            .off('shown.bs.modal')
                            .on('shown.bs.modal', function () {
                                $list.ekynaTable({
                                    onSelection: function (elements) {
                                        params.onSelection(elements);
                                        $modal.modal('hide');
                                    }
                                });
                            });

                        $modal.find('.modal-body').html($list);
                        $modal.modal({show: true});
                    }
                });
            });
        });

        return this;
    };

})(window.jQuery);