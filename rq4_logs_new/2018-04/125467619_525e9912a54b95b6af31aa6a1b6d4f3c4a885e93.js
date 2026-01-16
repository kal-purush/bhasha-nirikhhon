
$("#add-fun-gallery").click(function(){
		$(".fun-table-bckend").append('<tr class="event-gallery"><th>Event Gallery</th><td><input  name="event_gallery[]" type="file" accept="image/*"></td></tr>');
});


$("#add-more-update").click(function(){
	//alert('786');
		$(".fun-table-bckend").append('<tr class="event-gallery"><th>Add Gallery Image</th><td><input  name="event_gallery[]" type="file" accept="image/*"></td></tr>');
});


$(document).ready(function(){

 	$("#fun").click(function(){

		$(this).addClass('active');

	});


	$("#contact").click(function(){

		$(this).addClass('active');
		
	});

});

 


 $(function () {
	$('.popup-modal').magnificPopup({
		type: 'inline',
		preloader: false,
		focus: '#username',
		modal: true
	});
	$(document).on('click', '.popup-modal-dismiss', function (e) {
		e.preventDefault();
		$.magnificPopup.close();
	});
});  