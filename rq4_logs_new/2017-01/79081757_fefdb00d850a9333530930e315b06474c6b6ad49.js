// This is a manifest file that'll be compiled into application.js, which will include all the files
// listed below.
//
// Any JavaScript/Coffee file within this directory, lib/assets/javascripts, vendor/assets/javascripts,
// or any plugin's vendor/assets/javascripts directory can be referenced here using a relative path.
//
// It's not advisable to add code directly here, but if you do, it'll appear at the bottom of the
// compiled file.
//
// Read Sprockets README (https://github.com/rails/sprockets#sprockets-directives) for details
// about supported directives.
//
//= require jquery
//= require jquery_ujs
//= require turbolinks
//= require foundation.min

$(document).foundation();

$(document).ready(function(){
	var toggler = 0;
    $("#toggler").click(function(){
    	if (toggler == 0) {
    		$(".more").fadeIn();
    		toggler = 1;
    	} else {
        	$(".more").fadeOut();
        	toggler = 0;
    	}

    });
});