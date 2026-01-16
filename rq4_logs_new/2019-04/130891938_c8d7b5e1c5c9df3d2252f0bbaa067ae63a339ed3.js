$('.collapse').on('show.bs.collapse', function () {
	if (!$(this).parent().parent().hasClass('accordionToggle'))
  		$(this).parent().parent().find('.panel-collapse').each(function(){  		
  			$(this).collapse('hide');
  });
})