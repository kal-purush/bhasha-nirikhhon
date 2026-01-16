document.getElementsByTagName("NAV")[0].innerHTML ='<ul>'+
	'<li><a href="./">Home</a></li>'+
		'<li><a href="directories">Directories and Files</a></li>'+
		'<li><a href="lettersnumbers">Letters and Numbers</a></li>'+
		'<li><a href="rubybits">Ruby Tidbits</a></li>'+
		'<li><a href="identifiers">Ruby Identifiers</a></li>'+
		'<li><a href="resources">More Resources</a></li>'+
	'</ul>';

document.getElementById("mobilenav").innerHTML ='<ul>'+
	'<li><a href="./">Home</a></li>'+
		'<li><a href="directories">Directories and Files</a></li>'+
		'<li><a href="lettersnumbers">Letters and Numbers</a></li>'+
		'<li><a href="rubybits">Ruby Tidbits</a></li>'+
		'<li><a href="identifiers">Ruby Identifiers</a></li>'+
		'<li><a href="resources">More Resources</a></li>'+
	'</ul>';

document.getElementsByTagName("FOOTER")[0].innerHTML = '<h4>Ruby Wars '+currentYear()+ ' - Website by <a href="http://www.tinaellisbend.com/" target="_blank" />Tina Ellis</a></h4>';

//change specific element by ID
//document.getElementById('{id goes here - case sensitive}').innerHTML = '';
//document.getElementById('')

function currentYear(){
	var date=new Date();
	var year=date.getFullYear();
	return year;
}
