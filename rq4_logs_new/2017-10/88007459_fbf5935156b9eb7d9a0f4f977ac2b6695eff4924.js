$(document).ready(function() {
	$("#contact-submit").on("click", function(event) {
		event.preventDefault();

	  // Change to your service ID, or keep using the default service
	  var service_id = "outlook";
	  var template_id = "template_zXmhUPKK";

	  $(this).find("#contact-submit").text("Sending...");
	  emailjs.sendForm(service_id,template_id,"myform")
	  	.then(function(){ 
	    	alert("Sent!");
	       myform.find("#contact-submit").text("Send");
	    }, function(err) {
	       alert("Send email failed!\r\n Response:\n " + JSON.stringify(err));
	       myform.find("#contact-submit").text("Send");
	    });
	  return false;
	});
}); 