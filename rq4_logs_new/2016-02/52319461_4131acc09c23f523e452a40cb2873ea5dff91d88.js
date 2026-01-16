window.onload=function() {
	var msg = document.getElementById('msg');
	if(msg.value != "") {
		alert(msg.value);
		return true;
	}else{
		return false;	
	}
};