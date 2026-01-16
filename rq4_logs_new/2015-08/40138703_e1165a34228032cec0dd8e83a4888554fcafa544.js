$(document).ready(function() {

      var $error = $('.error');
      var $inputPlayer = $('#playerInput');

            $inputPlayer.click(function() {
            $(this).val("");
             $('.error').fadeOut(100);
      });



      $error.on({
            'load': function() {
                  $(this).show();
            },
            'click': function() {
                  $(this).fadeOut(200);
            }

      })


});