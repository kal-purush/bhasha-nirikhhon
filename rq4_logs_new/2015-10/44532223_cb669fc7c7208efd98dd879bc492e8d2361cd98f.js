var employeeArray = [];

$(document).ready(function(){
	$("#employeeinfo").submit(function(event){
		event.preventDefault();

		var values = {};

		console.log($("#employeeinfo").serializeArray());
		$.each($("#employeeinfo").serializeArray(), function(i, field){
			values[field.name] = field.value;  // im not sure what the [] are doing. looks like its creating an object
		})
		
		$("#employeeinfo").find("input[type=text]").val("");
		console.log(values);
		employeeArray.push(values);
		appendDom(values);


	});
});

function appendDom(employee){
	console.log(employee);
	$("#employeeContainer").append("<div class='employee'></div>");
	var $el = $("#employeeContainer").children().last();

	$el.append("<p>" + employee.employeename + "</p>");
	$el.append("<p>" + employee.employeenumber + "</p>");
	$el.append("<p>" + employee.jobtitle+"</p>" );
	$el.append("<p>" + employee.salary+"</p>" );
	

}



///First we have to add more inputs- done with inputs
////job title and salary
///take all the employees salaries  and figure out what the montly cost of the those employees cost


// Hard mode
///create a delte button that removes the employee from the form


//Pro mode

////update total salarys discount the removed employees salary
// jQuery's .data() function to help complete this. Note, you will need to do 
//something both when the employee is added and when they are deleted to make your application 'smart'.