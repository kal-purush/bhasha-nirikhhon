let player=1;

function makewinner(p1,p2,p3){
	let b1 = document.querySelector(p1);
	let b2 = document.querySelector(p2);
	let b3 = document.querySelector(p3);
	let h3 = document.querySelector("h3");
	h3.innerHTML="We Have Winner"
	b1.className="bwin";
	b2.className="bwin";
	b3.className="bwin";
}
function check(){
	let b1 = document.querySelector("#b1");
	let b2 = document.querySelector("#b2");
	let b3 = document.querySelector("#b3");
	let b4 = document.querySelector("#b4");
	let b5 = document.querySelector("#b5");
	let b6 = document.querySelector("#b6");
	let b7 = document.querySelector("#b7");
	let b8 = document.querySelector("#b8");
	let b9 = document.querySelector("#b9");
	if (b1.value == b2.value && b2.value==b3.value && b3.value !="E") {makewinner("#b1","#b2","#b3")}
	if (b1.value == b4.value && b4.value==b7.value && b7.value !="E") {makewinner("#b1","#b4","#b7")}
	if (b1.value == b5.value && b5.value==b9.value && b9.value !="E") {makewinner("#b1","#b5","#b9")} 
	if (b2.value == b5.value && b5.value==b8.value && b8.value !="E") {makewinner("#b2","#b5","#b8")}
	if (b3.value == b5.value && b5.value==b7.value && b7.value !="E") {makewinner("#b3","#b5","#b7")}
	if (b3.value == b6.value && b6.value==b9.value && b9.value !="E") {makewinner("#b3","#b6","#b9")}
	if (b4.value == b5.value && b5.value==b6.value && b6.value !="E") {makewinner("#b4","#b5","#b6")}
	if (b7.value == b8.value && b8.value==b9.value && b9.value !="E") {makewinner("#b7","#b8","#b9")}   
}
function AddForme(postion) {
	let item = document.querySelector(postion);
	if(item.value=="E"){
		if(player==1){
		item.value="O";
		item.className="btn2";
		player=2;
		check();
	}
	else{
		item.value="X";
		item.className="btn2"
		player=1;
		check();
	}
	}
}