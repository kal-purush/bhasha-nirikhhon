$(document).ready(function() {
	$('#loginForm').on('submit',function(event) {
		event.preventDefault();
	    // get the form data
	    // there are many ways to get this data using jQuery (you can use the class or id also)
	    var formData = {
	        'user'              : $('input[name=user]').val(),
	        'password'          : $('input[name=password]').val()
	    };
	
	
	    $.ajax({
	        type: 'POST', // define the type of HTTP verb we want to use (POST for our form)
	        url: 'LoginServlet', // the url where we want to POST
	        data: formData, // our data object
	        dataType: 'json', // what type of data do we expect back from the server
	        encode: true
	    })
	        // using the done promise callback
	        .done(function (data) {
	
	            // here we will handle errors and validation messages
	            if (data.status == 'Denied') 
	            {                      
	            	$('input[name=user]').val("");
	                $('input[name=password]').val("");
	                alert(data.message);
	                //$('#ErrorMessage').text(data.message);
	            }
	            else{
	            	if(data.status=='OK'){
	            		location.href="index.jsp";
	            	}
	            }
	            
	        });
	    
	});
});