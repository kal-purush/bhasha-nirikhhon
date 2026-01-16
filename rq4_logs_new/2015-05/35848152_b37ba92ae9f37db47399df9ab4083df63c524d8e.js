jQuery(function($) {

  function checkZip(zip) {
  
    // Check w server to validate the zipcode
    $.ajax({
      type: 'GET',
      url: '/ajax/rfi_process_zip/'+zip,
      dataType: 'json',
      success: function (data) {
        var city_state_zip = data;

        // Check that we got city and state back
        if( city_state_zip['city'] == '' || city_state_zip['city'] === undefined || city_state_zip['state'] == '' || city_state_zip['state'] === undefined ) {
          $('#edit-go').attr("disabled", "disabled"); // Disable GO button
        } else {
          $('#edit-go').prop("disabled", false); // Enable GO button
        }
      }
    });
  }
  
});