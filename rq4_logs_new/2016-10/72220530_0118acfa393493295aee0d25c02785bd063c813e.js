$(document).ready(function() {
    $('#summernote').summernote({height: 300, codemirror: {theme: 'monokai'}});
});

var postForm = function() {
    var content = $('textarea[name="content"]').html($('#summernote').code());
}