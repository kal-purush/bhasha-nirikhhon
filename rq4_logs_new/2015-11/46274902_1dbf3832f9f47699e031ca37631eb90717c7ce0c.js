var createPlayer = function(){
	if(document.querySelector('#name').value==''){
		alert('first enter your name')
	}
	else{
		req = new XMLHttpRequest();
		req.onreadystatechange = function() {
			if(req.readyState == 4 && req.status ==200) {
				alert(req.responseText);
			}
		}
		req.open('POST','player',true);
		req.send('name='+document.querySelector('#name').value);
	}
		document.getElementsByTagName('button')[0].style.display='inline';
}