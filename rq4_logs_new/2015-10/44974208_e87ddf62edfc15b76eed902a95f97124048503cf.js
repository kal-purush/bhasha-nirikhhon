var helper = {
	setStorage : function(key,value){
		localStorage.setItem(key,JSON.stringify(value));
	},
	getStorage : function(key){
		return JSON.parse(localStorage.getItem(key));
	},
	isElementInViewport : function(elem){
		var $elem = $(elem);

		// Get the scroll position of the page.
		var scrollElem = ((navigator.userAgent.toLowerCase().indexOf('webkit') != -1) ? 'body' : 'html');
		var viewportTop = $(scrollElem).scrollTop();
		var viewportBottom = viewportTop + $(window).height();

		// Get the position of the element on the page.
		var elemTop = Math.round( $elem.offset().top );
		var elemBottom = elemTop + $elem.height();

		return ((elemTop < viewportBottom) && (elemBottom > viewportTop));
	},
	toType : function(obj){
		return ({}).toString.call(obj).match(/\s([a-zA-Z]+)/)[1].toLowerCase();
	},
	baseUrl : function(window){
		return window.location.protocol + "//" + window.location.host + "/" + window.location.pathname.split('/')[1];
	},
	inputsValueArray : function(parent,inputType) {
		return $(parent).find(inputType).map(function() {
			return this.value;
		}).get();
	},
	inputsNameValObject : function(parent,inputTypeArray) {
		temp = {};
		for(input in inputTypeArray){
			$(parent).find(inputTypeArray[input]).map(function() {
				temp[this.name] = $(this).val();
			}).get();
		}
		return temp;
	},
	specifyOther : function(obj){
		if ($(obj).val() == 'other') {
			$(obj).next('.display_other').slideToggle(500);
		} else{
			$(obj).next('.display_other').slideUp(500);
		};
	}
}