jQuery(document).ready(function(){
	
	var url = window.location.href;
	var slide = url.substring(url.lastIndexOf('/')+2); // 4
	
	// Remove the trailing slash if necessary
	if( slide.indexOf("/") > -1 ) { 
	    var slideRmvSlash = slide.replace('/','');
	    slide = parseInt(slideRmvSlash);
	}
	slide = parseInt(slide);
	if (slide) {
	goToSlide(slide);
	}
	
	jQuery('[data-toggle="tooltip"]').tooltip();
	
});




function goToSlide(number) {
   jQuery("#carouselExampleControls").carousel(number);
}

jQuery('.thumbnail-image').on('click' , function () {
	var slidemove = 0;
	slidemove = this.getAttribute('change-slide-to');
	   jQuery("#wrapper-hero").collapse('show');
	   jQuery("#multic-2").collapse('hide');
	 jQuery("#carouselExampleControls").carousel(Number(slidemove));
	 });

jQuery('#multic-2').on('hidden.bs.collapse', function () {
  // do something…
  
  jQuery("#wrapper-hero").collapse('show');
});

jQuery('#multic-2').on('show.bs.collapse', function () {
  // do something…
  
  jQuery("#wrapper-hero").collapse('hide');
});
