import { DataTableEs } from '../datatable_lang.js';
import { Toast, ToastError, SwalDelete } from '../sweet_alerts.js';
import { setActiveCheckbox, setActiveSubmitButton } from '../../../common/js/utils.js';

$(function () {

    setActiveCheckbox('.label-status-checkbox', '.txt-status-label')

    var labelsTableEle = $('#labels-table');
    var getDataTable = labelsTableEle.data('url');
    var labelsTable = labelsTableEle.DataTable({
        language: DataTableEs,
        responsive: true,
        serverSide: true,
        processing: true,
        ajax: getDataTable,
        columns: [
            { data: 'id', name: 'id' },
            { data: 'name', name: 'name' },
            { data: 'description', name: 'description' },
            { data: 'status', name: 'status', searchable: false},
            { data: 'created_at', name: 'created_at'},
            { data: 'updated_at', name: 'updated_at'},
            { data: 'action', name: 'action', orderable: false, searchable: false },
        ],
        order: [
            [0, 'desc']
        ]
    });


    // ------------ REGISTER ---------------

    var registerLabelForm = $('#registerLabelForm').validate({
        rules: {
            name: {
                required: true,
                maxlength: 100
            },
            description: {
                required: true,
                maxlength: 600
            },
        },
        submitHandler: function (form, event) {
            event.preventDefault()
            var form = $(form)
            var loadSpinner = form.find('.loadSpinner')
            var modal = $('#RegisterLabelModal')

            loadSpinner.toggleClass('active')
            form.find('.btn-save').attr('disabled', 'disabled')

            $.ajax({
                method: form.attr('method'),
                url: form.attr('action'),
                data: form.serialize(),
                dataType: 'JSON',
                success: function (data) {

                    if (data.success) {

                        registerLabelForm.resetForm()
                        labelsTable.draw()

                        form.trigger('reset')
                        modal.modal('hide')

                        Toast.fire({
                            icon: 'success',
                            text: data.message
                        })
                    }
                    else {
                        Toast.fire({
                            icon: 'error',
                            text: data.message
                        })
                    }

                },
                complete: function (data) {
                    form.find('.btn-save').removeAttr('disabled')
                    loadSpinner.toggleClass('active')
                },
                error: function (data) {
                    console.log(data)
                    ToastError.fire()
                }
            })
        }
    })


    // ----------- SHOW ------------

    $('html').on('click', '.view_label_btn', function () {
        var modal = $('#viewLabelModal')
        var form = modal.find('#viewLabelForm')
        var getDataUrl = $(this).data('url')

        $.ajax({
            type: 'GET',
            url: getDataUrl,
            dataType: 'JSON',
            success: function (data) {

                var label = data.label;

                $.each(label, function (key, value) {
                    var input = form.find('[name='+ key +']')
                    if (input) {
                        input.val(value)
                    }
                })

                form.find('.status-btn').html(label.status)

            },
            complete: function (data) {
                modal.modal('show')
            },
            error: function (data) {
                console.log(data)
            }
        })
    })

    // ------------- EDIT ------------

    var editLabelForm = $('#editLabelForm').validate({
        rules: {
            name: {
                required: true,
                maxlength: 100
            },
            description: {
                required: true,
                maxlength: 600
            },
        },
        submitHandler: function (form, event) {
            event.preventDefault()
            var form = $(form)
            var loadSpinner = form.find('.loadSpinner')
            var modal = $('#EditLabelModal')

            loadSpinner.toggleClass('active')
            form.find('.btn-save').attr('disabled', 'disabled')

            $.ajax({
                method: form.attr('method'),
                url: form.attr('action'),
                data: form.serialize(),
                dataType: 'JSON',
                success: function (data) {

                    if (data.success) {

                        editLabelForm.resetForm()
                        labelsTable.ajax.reload(null, false)

                        form.trigger('reset')
                        modal.modal('hide')

                        Toast.fire({
                            icon: 'success',
                            text: data.message
                        })
                    }
                    else {
                        Toast.fire({
                            icon: 'error',
                            text: data.message
                        })
                    }

                },
                complete: function (data) {
                    form.find('.btn-save').removeAttr('disabled')
                    loadSpinner.toggleClass('active')
                },
                error: function (data) {
                    console.log(data)
                    ToastError.fire()
                }
            })
        }
    })

    $('html').on('click', '.editLabel', function () {
        var modal = $('#EditLabelModal')
        var getDataUrl = $(this).data('send')
        var url = $(this).data('url')
        var form = modal.find('#editLabelForm')

        editLabelForm.resetForm()
        form.trigger('reset')

        form.attr('action', url)

        $.ajax({
            type: 'GET',
            url: getDataUrl,
            dataType: 'JSON',
            success: function (data) {

                var label = data.label;

                $.each(label, function (key, value) {
                    var input = form.find('[name='+ key +']')
                    if (input) {
                        input.val(value)
                    }
                })

                if (label.status == 1) {
                    form.find('.label-status-checkbox').prop('checked', true);
                    form.find('.txt-status-label').html('Activo');
                } else {
                    form.find('.label-status-checkbox').prop('checked', false);
                    form.find('.txt-status-label').html('Inactivo');
                }
            },
            complete: function (data) {
                modal.modal('show')
            },
            error: function (data) {
                console.log(data)
            }
        })
    })


    // ------------- DESTROY ---------------

     $('html').on('click', '.deleteLabel', function () {
        var url = $(this).data('url')

        SwalDelete.fire().then(function (e) {
            if (e.value === true) {
                $.ajax({
                    type: 'DELETE',
                    url: url,
                    dataType: 'JSON',
                    success: function (data) {

                        if (data.success) {

                            labelsTable.ajax.reload(null, false)

                            Toast.fire({
                                icon: 'success',
                                text: data.message,
                            })

                        } else {
                            Toast.fire({
                                icon: 'error',
                                text: data.message
                            })
                        }
                    },
                    error: function (data) {
                        ToastError.fire()
                    }
                });
            } else {
                e.dismiss;
            }
        }, function (dismiss) {
            return false;
        });
    })

})