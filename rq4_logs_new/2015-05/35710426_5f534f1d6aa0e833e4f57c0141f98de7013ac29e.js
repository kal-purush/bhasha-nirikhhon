var cnt=0;
var img_cnt = {"catimg0":0,"catimg1":0,"catimg2":0,"catimg3":0,"catimg4":0};


function clickCount(img_id){
	var elem = document.getElementById(img_id);
	displayCat(img_id,elem);
	img_cnt[img_id]++;
	alert( "You clicked on " +img_id +" Cat " + img_cnt[img_id] +" times ");
	var pic1 = document.getElementById("pic1");
	document.getElementById(img_id).style.display='none';
	document.getElementById(img_id).addEventListener("click",clickCount(img_id));

}

function displayCat(img_id,elem){
	var catDisplay = document.createElement("img");
	catDisplay.src = "catimg.jpg";
	if(elem == null){
		document.body.appendChild(catDisplay);
	}else{
		document.body.replaceChild(catDisplay,elem);
	     }
		
	
}


