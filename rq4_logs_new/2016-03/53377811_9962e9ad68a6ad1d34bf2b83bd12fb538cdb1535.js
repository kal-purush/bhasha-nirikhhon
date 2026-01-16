$(function(){
	var searchDBString = [
		{value: "Hello", url: "expression.html#hello"},
		{value: "Coffee",url: "expression.html#coffee"},
		{value:"Bye", url: "expression.html#bye"},
		{value:"Love", url: "expression.html#love"},
		{value:"Sorry", url: "expression.html#sorry"},
		{value:"Name", url: "expression.html#name"},
		{value:"Thank You", url: "expression.html#thankyou"},
	];
	$("#autocomplete1").autocomplete({
		source: searchDBString,
		select: function(event, ui){
			window.location = ui.item.url;
	}
	});

});