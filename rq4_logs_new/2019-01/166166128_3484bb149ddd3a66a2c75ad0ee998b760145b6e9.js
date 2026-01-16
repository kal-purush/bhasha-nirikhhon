<script>
//var data1=JSON.parse()
//var data2=JSON.stringify()
var data=[["INT022","ravi","Sharma","ravi.sharma@cygrp.com","8449961949","09-11-96","Intern"],["INT022","ravi","Sharma","ravi.sharma@cygrp.com","8449961949","09-11-96","Intern"],["INT02","risabh","Singhal","risabh.singhal@cygrp.com","84496565645","01-10-97","Intern"]];
table=document.getElementById("mytable");
	for (var i=0; i<data.length; i++){
		var row=table.insertRow(table.length);
			for (var j=0; j<data[i].length; j++){
				var column=row.insertCell(j);
				column.innerHTML=data[i][j];
			}
	}
</script>