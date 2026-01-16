function loadPage(pageName){
	//Load does not work because we have to enable the class after the file has been downloaded.
	//So we use get
	//$('#topPanel').load('header.html');
	$.get('header.html', function(data) {
		$( "#topPanel").html( $(data) );
		var selector='#'+pageName + ' a';
		$(selector).attr("href","#");
		$(selector).addClass("active");
	});
	
}