(function() {
  'use strict';

  window.thoughter = window.thoughter || {};

  $('.post-new-thought')
      .on('submit', function postThought (event) {
        event.preventDefault();
        console.log('submit works');
        window.thoughter.createThought($('.form-control').val());
      });
}());