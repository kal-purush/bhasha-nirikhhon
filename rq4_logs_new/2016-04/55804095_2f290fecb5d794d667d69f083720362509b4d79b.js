function fixHeight(obj, offset) {
	var helpFrame = jQuery("#" + obj.name);
	jQuery(window).resize(function(ev){
		var innerDocWidth = helpFrame.width();
		helpFrame.height((innerDocWidth/(16/9)) + offset);
	});
	var innerDocWidth = helpFrame.width();
	helpFrame.height((innerDocWidth/(16/9)) + offset);
}