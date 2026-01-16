$(document).ready(function(){
  var url = "https://maps.googleapis.com/maps/api/place/textsearch/json?location=-29.650203500000003,-82.3237312&query=";

  var getDescription = function(name){
    $.get({
      url: url + name + "&key=AIzaSyCio1lbC-bpZGEb7FEcwBryRGpVlLe96LQ",
    }).done(function(response){
      var exact_name1 = response.results[0].name;
      var exact_name2 = response.results[1].name;
      var exact_name3 = response.results[2].name;

      var address = response.results[0].formatted_address


      // $('#list option:selected').text(exact_name1);
      // $("#list").val("<option value>");
      // $('#list option:selected').text(exact_name2);
      // $("#list").val("place3");
      // $('#list option:selected').text(exact_name3);

      $('#place_googlename').append($('<option>', {
      value: 1,
      text: exact_name1
      }));

      $('#place_googlename').append($('<option>', {
      value: 2,
      text: exact_name2
      }));

      $('#place_googlename').append($('<option>', {
      value: 3,
      text: exact_name3
      }));

      $.ajax({
        url : "/places/new",
        type : "GET",
        data : { place1: exact_name1,  place2: exact_name2, place3: exact_name3}
    });
      // $("#place_image_uri").val(exact_name);
    });
  };



  $('#place_name').change(function(){
    getDescription(this.value);
  });

  $('#place_googlename').change(function(){
    var name = $('#place_googlename option:selected').text();
    var url = "https://maps.googleapis.com/maps/api/place/textsearch/json?location=-29.650203500000003,-82.3237312&query=";

    $.get({
      url: url + name + "&key=AIzaSyCio1lbC-bpZGEb7FEcwBryRGpVlLe96LQ",
    }).done(function(response){
      $('#place_address').val(response.results[0].formatted_address);
      $('#place_lat').val(response.results[0].geometry.location.lat);
      $('#place_lng').val(response.results[0].geometry.location.lng);

      photo_reference = response.results[0].photos[0].photo_reference
      alert(photo_reference);

      image_url = "https://maps.googleapis.com/maps/api/place/photo?maxwidth=400&photoreference=" + photo_reference + "&key=Fakekey"
      $('#place_image_uri').val(image_url);
      // ---------------------------------------------------------
      // $.get({
      //   url: "https://maps.googleapis.com/maps/api/place/photo?maxwidth=400&photoreference=" + photo_reference + "&key=AIzaSyCio1lbC-bpZGEb7FEcwBryRGpVlLe96LQ",
      // }).done(function(response_image){
      //
      //   $('#place_image_uri').val(response_image);
      // });

      // ---------------------------------------------------------

    });



    });

  });