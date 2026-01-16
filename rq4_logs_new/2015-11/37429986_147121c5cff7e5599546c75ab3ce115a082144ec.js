// Applicant Roles
var idx = 1;

var getRandomInt = function (min, max) {
  return Math.floor(Math.random() * (max - min)) + min;
};
var repeatFx = function (fn, times) {
    for(var i = 0; i < times; i++)fn();
}

$("#ApplicantEntityRole_" + idx + " input[type=checkbox]").prop("checked", false);
var numRoles = $("#ApplicantEntityRole_" + idx + " input[type=checkbox]").length;
var tickRole = function() { 
    $("#ApplicantEntityRole_" + idx + "_" + getRandomInt(1, numRoles) ).prop('checked', true );
};
repeatFx( tickRole, 3 );