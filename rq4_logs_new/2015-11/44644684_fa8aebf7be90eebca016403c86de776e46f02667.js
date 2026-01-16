

var $overlay = $('<div id="overlay"></div>');
var $img = $("<img>");

$overlay.append($img);
$("body").append($overlay);

var main = function() {
	$("#gallery a").click(function(event) {
		event.preventDefault();
		var href = $(this).attr("href");
		$img.attr("src", href);
		$overlay.show();
	})	
	$overlay.click(function() {
		$(this).hide();
	})	
}
