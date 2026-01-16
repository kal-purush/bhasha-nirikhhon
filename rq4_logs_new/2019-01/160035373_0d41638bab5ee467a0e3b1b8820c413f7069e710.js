
$(document).ready(function () {
  'use strict'
    $('[id$="address_attributes_zipcode"]').on('change', function () {
      $.ajax({
            url:  "/filter_units_by_postcode",
            type: "GET",
            data: { selected_postcode: $(this).val(),address_type:  $(this).data("addressid")},
            success: function(data){
               //console.log(data)
             }
        });
    })
  //console.log("OK");
})
;
$(document).ready(function () {
  'use strict'

  if ($('#new_district_link').length) {
    $('#state').on('change', function () {
      var newStateLinkHref = $('#new_district_link').prop('href')
      var selectedCountryId = $('#state option:selected').prop('value')
      var newLink = newStateLinkHref.replace(/states\/(\d+)/,
        'states/' + selectedCountryId)
      $('#new_district_link').attr('href', newLink)
    })
  };
  //console.log("OK");
})
;
$(document).ready(function () {
  'use strict'

  if ($('#new_subdistrict_link').length) {
    $('#district').on('change', function () {
      var newStateLinkHref = $('#new_subdistrict_link').prop('href')
      var selectedCountryId = $('#district option:selected').prop('value')
      var newLink = newStateLinkHref.replace(/districts\/(\d+)/,
        'districts/' + selectedCountryId)
      $('#new_subdistrict_link').attr('href', newLink)
    })
  };
  //console.log("OK");
})
;
(function() {


}).call(this);
// This is a manifest file that'll be compiled into application.js, which will include all the files
// listed below.
//
// Any JavaScript/Coffee file within this directory, lib/assets/javascripts, vendor/assets/javascripts,
// or any plugin's vendor/assets/javascripts directory can be referenced here using a relative path.
//
// It's not advisable to add code directly here, but if you do, it'll appear at the bottom of the
// compiled file.
//
// Read Sprockets README (https://github.com/rails/sprockets#sprockets-directives) for details
// about supported directives.
//

;