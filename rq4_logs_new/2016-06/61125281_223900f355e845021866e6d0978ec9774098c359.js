window.addEventListener("load", function(event){
	var submit  = document.getElementById("submit");
    submit.addEventListener("click",sendRecord,false);
},false);

function sendRecord(){
		var success = document.getElementById("success");
		success.style.display = "none";
		
		var record = document.getElementById("record");
		record.style.display = "block";
		
		var name  = document.getElementById("name").value;
		var xhttp = new XMLHttpRequest();
		xhttp.overrideMimeType('json');
		xhttp.onreadystatechange = function() {
			if (xhttp.readyState == 4 && xhttp.status == 200) {
				var record = JSON.parse(xhttp.responseText);
				var html = "";
				for(var i=0;i<record.length;i++){
					html+="<p><b>第"+(i+1)+"名<br>  姓名:"+record[i].name+" 破關秒數:"+record[i].second+" 時間:"+record[i].time+"</p>";
				}
				document.getElementById("record_content").innerHTML = html;
			}
		};
		xhttp.open("GET", "http://140.121.196.20:7779/MSTT/RecordWinner?name="+name+"&second=1", true);
		xhttp.send();
}