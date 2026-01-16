function check(){
	let mode = document.frm.mode.value;
	if(document.frm.id.value.length == 0){
		alert("선수ID가 입력되지 않았습니다.");
		document.frm.id.focus();
		return false;
	}
	if(document.frm.name.value.length == 0){
		alert("선수이름이 입력되지 않았습니다.");
		document.frm.name.focus();
		return false;
	}
	if(document.frm.po.value =="0"){
		alert("포지션이 입력되지 않았습니다.");
		return false;
	}
	//console.log(document.frm.po.value == "0");
	if(document.frm.day.value.length == 0){
		alert("선수등록일이 입력되지 않았습니다.");
		document.frm.day.focus();
		return false;
	}
	if(document.frm.team.value == "0"){
		alert("소속팀이 선택되지 않았습니다.");
		document.frm.team.focus();
		return false;
	}
	success(mode);
}
function success(data){
	if(data == "add"){
		alert("선수등록이 정상적으로 완료되었습니다.");
		return true;	
	}
	else{
		alert("등록된 선수 정보 수정이 완료되었습니다.");
		return true;
	}
}

function Re(){
	alert("정보를 지우고 처음부터 다시 입력합니다!");
}