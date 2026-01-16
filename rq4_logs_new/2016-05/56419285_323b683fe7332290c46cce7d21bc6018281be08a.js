var valid = function(){		
	var uname = frm.uname.value;//获取表单控件值
	var unameTip=document.getElementById("uname");//获取元素对象（提示信息）
	if(/^[a-zA-z]\w{5,17}$/.test(uname)==false){
		unameTip.innerHTML="用户名非法";
		frm.uname.focus();//获取当前输入框的焦点
		return;
	}else{
		unameTip.innerHTML="用户名合法";
	}
	var pwd = frm.pwd.value;
	var pwdTip=document.getElementById("pwd");
	if(pwd==""){
		pwdTip.innerHTML="密码非法";
		frm.pwd.focus();
		return;
	}else{
		pwdTip.innerHTML="密码合法";
	}
	var pwd2 = frm.pwd2.value;
	var pwd2Tip=document.getElementById("pwd2");
	if(pwd!=pwd2){
		pwd2Tip.innerHTML="两次输入密码不一致";
		frm.pwd2.focus();
		return;
	}else{
		pwd2Tip.innerHTML="";
	}
	frm.submit();//提交表单
};