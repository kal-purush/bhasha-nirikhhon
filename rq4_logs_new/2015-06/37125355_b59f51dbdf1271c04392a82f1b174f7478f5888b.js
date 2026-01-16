$('#add_prog').on('click', function(){
	$('#modal').bPopup({
	        speed: 650,
	        transition: 'slideDown',
	        onClose: function () {
	          this.find('.form').trigger("reset"); // сбрасываем форму
	        }
	      });
});