$(document).ready(function(){

// Initialize Firebase
var config = {
  apiKey: "AIzaSyB136-lRPXhjYZ6P4mYunhM2x1B-ITW8Bw",
  authDomain: "employee-database-c6a6f.firebaseapp.com",
  databaseURL: "https://employee-database-c6a6f.firebaseio.com",
  storageBucket: "employee-database-c6a6f.appspot.com",
  messagingSenderId: "364340552009"
};

firebase.initializeApp(config);
var database = firebase.database();
var newEmployee = {};

// empty employee table except header row
$("#employees").find("tr:gt(0)").remove();

database.ref().on("child_added", function(childSnapshot){
 addEmployee(childSnapshot.val());

})

$("#submit").on("click",function(){
   event.preventDefault ();
   newEmployee.name = $("#empName").val().trim();
   newEmployee.role = $("#empRole").val().trim();
   newEmployee.startDate = $("#empDate").val().trim();
   newEmployee.rate = $("#empRate").val().trim();
 database.ref().push(newEmployee);
 $("#empName").val('');
 $("#empRole").val('');
 $("#empDate").val('');
 $("#empRate").val('');
});

function addEmployee(emp){
 var tBody = $("<tbody>");
 var newRow=$("<tr>");
 tBody.append(newRow);

 var startDate = moment(new Date(emp.startDate));
 var monthsWorked = parseInt(startDate.diff(moment(),'months')*-1);
 $("#employees").append(tBody);
 newRow.append("<td>"+
   emp.name+"</td><td>"+
   emp.role+"</td><td>"+
   emp.startDate+"</td><td>"+
   monthsWorked+"</td><td>$"+
   emp.rate+"</td><td>$"+monthsWorked*emp.rate+"</td>"
 )
}



}); //end ready function