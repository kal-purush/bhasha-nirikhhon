$(function () {

    // We can attach the `fileselect` event to all file inputs on the page
    $(document).on('change', ':file', function () {
        var input = $(this),
            numFiles = input.get(0).files ? input.get(0).files.length : 1,
            label = input.val().replace(/\\/g, '/').replace(/.*\//, '');
        input.trigger('fileselect', [numFiles, label]);
    });

    // We can watch for our custom `fileselect` event like this
    $(document).ready(function () {
        $('input[name=file_name_txt]')
            .ready(function () {
                var input = $('input[name=file_name_txt]');
                input.val('select a file...');
        });
        $(':file')
            .on('fileselect', function (event, numFiles, label) {

            var input = $(this).parents('.input-group').find(':text');
                console.log(input);
            if (input.length) {
                input.val(label);
            } else {
                input.val('select a file...');
            }

        });
    });

});