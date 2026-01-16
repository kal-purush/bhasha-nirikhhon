window.onload=function() {
    secObj = document.getElementById('showscs');
    minObj = document.getElementById('showmns');
    document.getElementById('btnct').onclick = countdownTimer;
}

function countdownTimer() {
    var sec = new Number(secObj.value);
    var min = new Number(minObj.value);
	
document.getElementById("showmns").innerHTML = min;
}
