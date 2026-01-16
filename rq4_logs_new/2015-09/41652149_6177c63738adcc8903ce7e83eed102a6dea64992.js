var $ = function( id ) { return document.getElementById( id ); };

function getBuild(){
	var $ = function( id ) { return document.getElementById( prefix + id ); };
	var champ = $('champion').options[$('champion').selectedIndex]
	window.location.href = './' + champ
}