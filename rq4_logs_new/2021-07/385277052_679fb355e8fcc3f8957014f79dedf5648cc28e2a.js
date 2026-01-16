$("#interventions_buildingid").prop("disabled", true); // second dropdown is disable while first dropdown is empty
$("#interventions_customerid").change(function(){
  	var customer = $(this).val();
  	if(customer == ''){
  		$("#interventions_buildingid").prop("disabled", true);
  	}else{
  		$("#interventions_buildingid").prop("disabled", false);
  	}
  	$.ajax({
	    url: "/get_building",
	    method: "GET",  
	    dataType: "json",
	    data: {customer: customer},
	    error: function (xhr, status, error) {
	      	console.error('AJAX Error: ' + status + error);
	    },
	    success: function (response) {
	      	console.log(response);
	      	var building = response["building"];
	      	$("#interventions_buildingid").empty();

	      	$("#interventions_buildingid").append('<option>Select Task</option>');
	      	for(var i = 0; i < building.length; i++){
	      		$("#interventions_buildingid").append('<option value="' + building[i]["id"] + '">' +building[i]["name"] + '</option>');
	      	}
	    }
  	});
});