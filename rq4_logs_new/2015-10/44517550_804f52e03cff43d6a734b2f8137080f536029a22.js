$(document).ready(function () {
  $('#registrationForm').submit(function (event) {
    event.preventDefault();
    var object = {};
    $(this).serializeArray().map(function (x) {
      object[x.name] = x.value;
    });
    console.log(object);
    API.RegisterUser(object, function (data) {
      console.log(data);
    });
    return false;
  });
});