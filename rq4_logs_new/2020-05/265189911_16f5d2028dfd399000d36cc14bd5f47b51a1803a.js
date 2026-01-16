(function() {
  'use strict';
  
  $('#fileupload').fileupload({
    dataType: 'json',
    type: 'POST',
    url: '//jquery-file-upload.appspot.com/',
    add: function (e, data) {
      $('#filename').text(data.files[0].name);
      $('#upload-status').text("Uploading...");
      setProgressBar(0);
      data.submit();
    },
    progressall: function (e, data) {
      setProgressBar(parseInt(data.loaded / data.total * 100, 10));
    },
    done: function (e, data) {
      $('#upload-status').text("Uploading Complete!");
    }
  });
  
  function setProgressBar(percentage) {
    $('#progress .bar').css(
      'width',
      percentage + '%'
    ).text(percentage + '%');
  }

}).call(this);