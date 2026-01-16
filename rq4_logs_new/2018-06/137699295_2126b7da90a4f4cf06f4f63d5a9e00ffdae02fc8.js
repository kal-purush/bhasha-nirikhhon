

$(document).ready(function() {

	//$(function() {
		$(".dropdown-menu li a").click(function(){
				var selText = $(this).text();
				$(this).parents('.dropdown').find('.dropdown-toggle').html(selText + ' <span class="caret"></span>');	
				$(this).parents('.dropdown').find('.dropdown-toggle').val(selText);
			});
		//});
});


  jQuery(window).ready(function() {
	    $("#estimateButton").click(function(event) {
	        
	        var pick = $('#pick').text();
	        var drop = $('#drop').text();
	        var cab = $('#cab').text();
	        var netId = $('#netId').val();
	        
	        if($('#pick').val()=="None"){
        		if(!($("#confirm").hasClass('disabled')))
        			$("#confirm").addClass('disabled');
        		$('#estimate').html('<p>'+'Select a Pickup Location '+'</p>');
            	$("#myModal").modal();
        	}
        	else if($('#drop').val()=="None"){
        		if(!($("#confirm").hasClass('disabled')))
        			$("#confirm").addClass('disabled');
        		$('#estimate').html('<p>'+'Select a Drop Location '+'</p>');
            	$("#myModal").modal();
        	}
        	else if($('#cab').val()=="None"){
        		if(!($("#confirm").hasClass('disabled')))
        			$("#confirm").addClass('disabled');
        		$('#estimate').html('<p>'+'Select a Cab Type '+'</p>');
            	$("#myModal").modal();
        	}
        	else if(pick == drop){
        		if(!($("#confirm").hasClass('disabled')))
        			$("#confirm").addClass('disabled');
        		$('#estimate').html('<p>'+'Pickup Location Cannot Be Same As Drop Location'+'</p>');
            	$("#myModal").modal();	
        	} 
	        
        	else{
	        jQuery.ajax({
	            url: "BookingController",
	            type: "post",
	            dataType: "text",
	            data: {
	                pick: pick,
	                drop: drop,
	                cab: cab,
	                netId : netId
	            },
	            success: function(data) {
	            	
	            	/*1 starts
	            	 else if( not enough balance){
	            	 	if(!($("#confirm").hasClass('disabled')))
	            			$("#confirm").addClass('disabled');
	            		$('#estimate').html('<p>'+'INSUFFICIENT CARD BALANCE'+'</p>');
		            	$("#myModal").modal();	
	            	}
	            	else if(cab type not available){
	            		if(!($("#confirm").hasClass('disabled')))
	            			$("#confirm").addClass('disabled');
	            		$('#estimate').html('<p>'+'Cab Type not available currently. Please try after some time or choose another cab type'+'</p>');
		            	$("#myModal").modal();	
	            		}
	            	*/
	            		if(($("#confirm").hasClass('disabled')))
	            			$("#confirm").removeClass('disabled');
	            		
		            	$('#estimate').html('<p>'+data+  '</br>' + "Please Confirm The Ride. " + '</p>');
		            	$("#myModal").modal();
		           	
	            	
	            }
	        });
        	}
	    });
});


  // G starts
  
  jQuery(window).ready(function() {
	    $("#confirm").click(function(event) {
	    	
	    	if(!($("#confirm").hasClass('disabled')))
	        {
	        var pick = $('#pick').text();
	        var drop = $('#drop').text();
	        var cab = $('#cab').text();
	        var netId = $('#netId').val();
	        
	        	      
      	
	        jQuery.ajax({
	            url: "MakeBookingController",
	            type: "post",
	            dataType: "text",
	            data: {
	                pick: pick,
	                drop: drop,
	                cab: cab,
	                netId : netId
	            },
	            success: function(data) {
	            	
	            /*	
	            	 if(data=="not enough balance"){
	           			$("#confirm").addClass('disabled');
	               		$('#estimate').html('<p>'+"You have insufficient balance to make this trip. Please re-charge and try again"+'</p>');
		            	$("#myModal").modal();	
	            	}
	            	else if(data=="cab type not available"){
	            		$("#confirm").addClass('disabled');
	                 	$('#estimate').html('<p>'+"The Cab type selected is not available at the moment. Please try after sometime"+'</p>');
		            	$("#myModal").modal();	
	            		}
	            	*/
	            		
	            	//else
	            	//	{
	                $('#estimate').html('<p>'+data+'</p>');
	            	$("#myModal").modal();
	            	//	}
	            			
		           	
	            	
	            }
	        
	        });
	        }
	    });
});