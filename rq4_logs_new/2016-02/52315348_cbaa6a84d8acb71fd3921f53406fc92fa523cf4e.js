$(document).ready(function(){
    jQuery.validator.setDefaults({
    debug: true,
    success: "valid"
  });

  $( "#my_form" ).validate({
    rules: {
      first_name: {
        required: true
      }
    }
  });
});