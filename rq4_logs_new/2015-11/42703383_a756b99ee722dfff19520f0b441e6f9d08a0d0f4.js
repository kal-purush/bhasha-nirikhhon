function checkEdit(){

	if (editform.content.value==""){
		alert("内容不可为空!");
		message_submit.content.focus();
		return false;
	}

	var str = editform.content.value;

	if (str.length < 15){
		alert("内容不可少于15字！");
		message_submit.content.focus();
		return false;
	}

}