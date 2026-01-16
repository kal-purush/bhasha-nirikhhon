// Wait to attach handler until the DOM is fully loaded.
$(function(){
	$(".devour-burger").on("click", function(event){
		// Set id based on button data-id attribute
		var id = $(this).data("id");
		// Send the PUT request to move burger to devoured list
		$.ajax("/api/burgers/"+id, {
			type: "PUT",
			data: id
		}).then(function(){
			// Reload the webpage after update to database
			location.reload();
		})
	});

});