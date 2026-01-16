//funzioni per scegliere l'immagine da visualizzare

function gestoreClickF1(){
  try {
    var nodoF1 = document.getElementById("fronteC1");
	var nodoR1 = document.getElementById("retroC1");
    nodoF1.style.display = "none";
	nodoR1.style.display = "block";		
	
  } catch (e) {
    alert("gestoreClickF1 " + e);
  }
}

function gestoreClickR1(){
  try {
    var nodoF1 = document.getElementById("fronteC1");
	var nodoR1 = document.getElementById("retroC1");
	nodoR1.style.display = "none";
    nodoF1.style.display = "block";
	
  } catch (e) {
    alert("gestoreClickR1 " + e);
  }
}

function gestoreClickF2(){
  try {
    var nodoF2 = document.getElementById("fronteC2");
	var nodoR2 = document.getElementById("retroC2");
    nodoF2.style.display = "none";
	nodoR2.style.display = "block";		
	
  } catch (e) {
    alert("gestoreClickF2 " + e);
  }
}

function gestoreClickR2(){
  try {
    var nodoF2 = document.getElementById("fronteC2");
	var nodoR2 = document.getElementById("retroC2");
	nodoR2.style.display = "none";
    nodoF2.style.display = "block";
	
  } catch (e) {
    alert("gestoreClickR2 " + e);
  }
}

function gestoreClickF3(){
  try {
    var nodoF3 = document.getElementById("fronteC3");
	var nodoR3 = document.getElementById("retroC3");
    nodoF3.style.display = "none";
	nodoR3.style.display = "block";		
	
  } catch (e) {
    alert("gestoreClickF3 " + e);
  }
}

function gestoreClickR3(){
  try {
    var nodoF3 = document.getElementById("fronteC3");
	var nodoR3 = document.getElementById("retroC3");
	nodoR3.style.display = "none";
    nodoF3.style.display = "block";
	
  } catch (e) {
    alert("gestoreClickR3 " + e);
  }
}


//funzioni per far scorrere la pagina e portarla direttamente alla cartolina desiderata
function gestoreScroll1(){
	try {
		window.scrollTo(0, 320);
	} catch (e) {
		alert("gestoreScroll1" + e);
	}
}

function gestoreScroll2(){
	try {
		window.scrollTo(0, 1045);
	} catch (e) {
		alert("gestoreScroll2" + e);
	}
}

function gestoreScroll3(){
	try {
		window.scrollTo(0, 1770);
	} catch (e) {
		alert("gestoreScroll3" + e);
	}
}


//funzioni per evidenziare o no il testo corrispondente a una parte della mappa
function gestoreMappa1Over(){
	switch (this.id) {
		case 'data1':
			d1.style.backgroundColor = "#993366";
			d1.style.color = "white";
			break;
		case 'messaggio1':
			m1.style.backgroundColor = "#993366";
			m1.style.color = "white";
			break;
		case 'editore1':
			e1.style.backgroundColor = "#993366";
			e1.style.color = "white";
			break;
		case 'indirizzo1':
			i1.style.backgroundColor = "#993366";
			i1.style.color = "white";
			break;
		case 'francobollo1':
			f1.style.backgroundColor = "#993366";
			f1.style.color = "white";
			break;
		case 'numero1':
			n1.style.backgroundColor = "#993366";
			n1.style.color = "white";
			break;
		case 'serie1':
			s1.style.backgroundColor = "#993366";
			s1.style.color = "white";
			break;
		case 'timbro1':
			t1.style.backgroundColor = "#993366";
			t1.style.color = "white";
			break;
}}

function gestoreMappa1Out(){
	switch (this.id) {
		case 'data1':
			d1.style.backgroundColor = "white";
			d1.style.color = "#001a33";
			break;
		case 'messaggio1':
			m1.style.backgroundColor = "white";
			m1.style.color = "#001a33";
			break;
		case 'editore1':
			e1.style.backgroundColor = "white";
			e1.style.color = "#001a33";
			break;
		case 'indirizzo1':
			i1.style.backgroundColor = "white";
			i1.style.color = "#001a33";
			break;
		case 'francobollo1':
			f1.style.backgroundColor = "white";
			f1.style.color = "#001a33";
			break;
		case 'numero1':
			n1.style.backgroundColor = "white";
			n1.style.color = "#001a33";
			break;
		case 'serie1':
			s1.style.backgroundColor = "white";
			s1.style.color = "#001a33";
			break;
		case 'timbro1':
			t1.style.backgroundColor = "white";
			t1.style.color = "#001a33";
			break;
}}

function gestoreMappa2Over(){
	switch (this.id) {
		case 'data2':
			d2.style.backgroundColor = "#993366";
			d2.style.color = "white";
			break;
		case 'messaggio2':
			m2.style.backgroundColor = "#993366";
			m2.style.color = "white";
			break;
		case 'editore2':
			e2.style.backgroundColor = "#993366";
			e2.style.color = "white";
			break;
		case 'indirizzo2':
			i2.style.backgroundColor = "#993366";
			i2.style.color = "white";
			break;
		case 'francobollo2':
			f2.style.backgroundColor = "#993366";
			f2.style.color = "white";
			break;
		case 'numero2':
			n2.style.backgroundColor = "#993366";
			n2.style.color = "white";
			break;
		case 'serie2':
			s2.style.backgroundColor = "#993366";
			s2.style.color = "white";
			break;
		case 'timbro2':
			t2.style.backgroundColor = "#993366";
			t2.style.color = "white";
			break;
}}

function gestoreMappa2Out(){
	switch (this.id) {
		case 'data2':
			d2.style.backgroundColor = "white";
			d2.style.color = "#001a33";
			break;
		case 'messaggio2':
			m2.style.backgroundColor = "white";
			m2.style.color = "#001a33";
			break;
		case 'editore2':
			e2.style.backgroundColor = "white";
			e2.style.color = "#001a33";
			break;
		case 'indirizzo2':
			i2.style.backgroundColor = "white";
			i2.style.color = "#001a33";
			break;
		case 'francobollo2':
			f2.style.backgroundColor = "white";
			f2.style.color = "#001a33";
			break;
		case 'numero2':
			n2.style.backgroundColor = "white";
			n2.style.color = "#001a33";
			break;
		case 'serie2':
			s2.style.backgroundColor = "white";
			s2.style.color = "#001a33";
			break;
		case 'timbro2':
			t2.style.backgroundColor = "white";
			t2.style.color = "#001a33";
			break;
}}

function gestoreMappa3Over(){
	switch (this.id) {
		case 'data3':
			d3.style.backgroundColor = "#993366";
			d3.style.color = "white";
			break;
		case 'censura':
			c3.style.backgroundColor = "#993366";
			c3.style.color = "white";
			break;
		case 'messaggio3':
			m3.style.backgroundColor = "#993366";
			m3.style.color = "white";
			break;
		case 'editore3':
			e3.style.backgroundColor = "#993366";
			e3.style.color = "white";
			break;
		case 'indirizzo3':
			i3.style.backgroundColor = "#993366";
			i3.style.color = "white";
			break;
		case 'francobollo3':
			f3.style.backgroundColor = "#993366";
			f3.style.color = "white";
			break;
		case 'numero3':
			n3.style.backgroundColor = "#993366";
			n3.style.color = "white";
			break;
		case 'serie3':
			s3.style.backgroundColor = "#993366";
			s3.style.color = "white";
			break;
		case 'timbro3':
			t3.style.backgroundColor = "#993366";
			t3.style.color = "white";
			break;
}}

function gestoreMappa3Out(){
	switch (this.id) {
		case 'data3':
			d3.style.backgroundColor = "white";
			d3.style.color = "#001a33";
			break;
		case 'censura':
			c3.style.backgroundColor = "white";
			c3.style.color = "#001a33";
			break;
		case 'messaggio3':
			m3.style.backgroundColor = "white";
			m3.style.color = "#001a33";
			break;
		case 'editore3':
			e3.style.backgroundColor = "white";
			e3.style.color = "#001a33";
			break;
		case 'indirizzo3':
			i3.style.backgroundColor = "white";
			i3.style.color = "#001a33";
			break;
		case 'francobollo3':
			f3.style.backgroundColor = "white";
			f3.style.color = "#001a33";
			break;
		case 'numero3':
			n3.style.backgroundColor = "white";
			n3.style.color = "#001a33";
			break;
		case 'serie3':
			s3.style.backgroundColor = "white";
			s3.style.color = "#001a33";
			break;
		case 'timbro3':
			t3.style.backgroundColor = "white";
			t3.style.color = "#001a33";
			break;
}}


//variabili
var nodoFronte1;
var nodoRetro1;
var nodoFronte2;
var nodoRetro2;
var nodoFronte3;
var nodoRetro3;
var nodoDescrizione;
var nodoTesto;
var cartolina1;
var cartolina2;
var cartolina3;
var data1;
var numero1;
var messaggio1;
var editore1;
var indirizzo1;
var francobollo1;
var serie1;
var timbro1;
var d1;
var n1;
var m1;
var e1;
var i1;
var f1;
var s1;
var t1;
var data2;
var numero2;
var messaggio2;
var editore2;
var indirizzo2;
var francobollo2;
var serie2;
var timbro2;
var d2;
var n2;
var m2;
var e2;
var i2;
var f2;
var s2;
var t2;
var data3;
var censura;
var numero3;
var messaggio3;
var editore3;
var indirizzo3;
var francobollo3;
var serie3;
var timbro3;
var d3;
var c3;
var n3;
var m3;
var e3;
var i3;
var f3;
var s3;
var t3;


function gestoreLoad(){
  try {
    nodoFronte = document.getElementById("fronteTC1");
    nodoRetro = document.getElementById("retroTC1");
    nodoFronte.onclick = gestoreClickR1;
    nodoRetro.onclick = gestoreClickF1;
	
	nodoFronte = document.getElementById("fronteTC2");
    nodoRetro = document.getElementById("retroTC2");
	nodoFronte.onclick = gestoreClickR2;
    nodoRetro.onclick = gestoreClickF2;
	
	nodoFronte = document.getElementById("fronteTC3");
    nodoRetro = document.getElementById("retroTC3");
	nodoFronte.onclick = gestoreClickR3;
    nodoRetro.onclick = gestoreClickF3;
	
	cartolina1 = document.getElementById("menu1");
	cartolina2 = document.getElementById("menu2");
	cartolina3 = document.getElementById("menu3");
	
	cartolina1.onclick = gestoreScroll1;
	cartolina2.onclick = gestoreScroll2;
	cartolina3.onclick = gestoreScroll3;
	
	data1 = document.getElementById("data1");
	numero1 = document.getElementById("numero1");
	messaggio1 = document.getElementById("messaggio1");
	editore1 = document.getElementById("editore1");
	indirizzo1 = document.getElementById("indirizzo1");
	francobollo1 = document.getElementById("francobollo1");
	serie1 = document.getElementById("serie1");
	timbro1 = document.getElementById("timbro1");
	
	d1 = document.getElementById("d1");
	n1 = document.getElementById("n1");
	m1 = document.getElementById("m1");
	e1 = document.getElementById("e1");
	i1 = document.getElementById("i1");
	f1 = document.getElementById("f1");
	s1 = document.getElementById("s1");
	t1 = document.getElementById("t1");
	
	data1.onmouseover = gestoreMappa1Over;
	numero1.onmouseover = gestoreMappa1Over;
	messaggio1.onmouseover = gestoreMappa1Over;
	editore1.onmouseover = gestoreMappa1Over;
	indirizzo1.onmouseover = gestoreMappa1Over;
	francobollo1.onmouseover = gestoreMappa1Over;
	serie1.onmouseover = gestoreMappa1Over;
	timbro1.onmouseover = gestoreMappa1Over;
	
	data1.onmouseout = gestoreMappa1Out;
	numero1.onmouseout = gestoreMappa1Out;
	messaggio1.onmouseout = gestoreMappa1Out;
	editore1.onmouseout = gestoreMappa1Out;
	indirizzo1.onmouseout = gestoreMappa1Out;
	francobollo1.onmouseout = gestoreMappa1Out;
	serie1.onmouseout = gestoreMappa1Out;
	timbro1.onmouseout = gestoreMappa1Out;
	
	
	data2 = document.getElementById("data2");
	numero2 = document.getElementById("numero2");
	messaggio2 = document.getElementById("messaggio2");
	editore2 = document.getElementById("editore2");
	indirizzo2 = document.getElementById("indirizzo2");
	francobollo2 = document.getElementById("francobollo2");
	serie2 = document.getElementById("serie2");
	timbro2 = document.getElementById("timbro2");
	
	d2 = document.getElementById("d2");
	n2 = document.getElementById("n2");
	m2 = document.getElementById("m2");
	e2 = document.getElementById("e2");
	i2 = document.getElementById("i2");
	f2 = document.getElementById("f2");
	s2 = document.getElementById("s2");
	t2 = document.getElementById("t2");
	
	data2.onmouseover = gestoreMappa2Over;
	numero2.onmouseover = gestoreMappa2Over;
	messaggio2.onmouseover = gestoreMappa2Over;
	editore2.onmouseover = gestoreMappa2Over;
	indirizzo2.onmouseover = gestoreMappa2Over;
	francobollo2.onmouseover = gestoreMappa2Over;
	serie2.onmouseover = gestoreMappa2Over;
	timbro2.onmouseover = gestoreMappa2Over;
	
	data2.onmouseout = gestoreMappa2Out;
	numero2.onmouseout = gestoreMappa2Out;
	messaggio2.onmouseout = gestoreMappa2Out;
	editore2.onmouseout = gestoreMappa2Out;
	indirizzo2.onmouseout = gestoreMappa2Out;
	francobollo2.onmouseout = gestoreMappa2Out;
	serie2.onmouseout = gestoreMappa2Out;
	timbro2.onmouseout = gestoreMappa2Out;
	
	data3 = document.getElementById("data3");
	censura = document.getElementById("censura");
	numero3 = document.getElementById("numero3");
	messaggio3 = document.getElementById("messaggio3");
	editore3 = document.getElementById("editore3");
	indirizzo3 = document.getElementById("indirizzo3");
	francobollo3 = document.getElementById("francobollo3");
	serie3 = document.getElementById("serie3");
	timbro3 = document.getElementById("timbro3");
	
	d3 = document.getElementById("d3")
	c3 = document.getElementById("c3");
	n3 = document.getElementById("n3");
	m3 = document.getElementById("m3");
	e3 = document.getElementById("e3");
	i3 = document.getElementById("i3");
	f3 = document.getElementById("f3");
	s3 = document.getElementById("s3");
	t3 = document.getElementById("t3");
	
	data3.onmouseover = gestoreMappa3Over;
	censura.onmouseover = gestoreMappa3Over;
	numero3.onmouseover = gestoreMappa3Over;
	messaggio3.onmouseover = gestoreMappa3Over;
	editore3.onmouseover = gestoreMappa3Over;
	indirizzo3.onmouseover = gestoreMappa3Over;
	francobollo3.onmouseover = gestoreMappa3Over;
	serie3.onmouseover = gestoreMappa3Over;
	timbro3.onmouseover = gestoreMappa3Over;
	
	data3.onmouseout = gestoreMappa3Out;
	censura.onmouseout = gestoreMappa3Out;
	numero3.onmouseout = gestoreMappa3Out;
	messaggio3.onmouseout = gestoreMappa3Out;
	editore3.onmouseout = gestoreMappa3Out;
	indirizzo3.onmouseout = gestoreMappa3Out;
	francobollo3.onmouseout = gestoreMappa3Out;
	serie3.onmouseout = gestoreMappa3Out;
	timbro3.onmouseout = gestoreMappa3Out;
	
  } catch (e) {
    alert("gestoreLoad " + e);
  }
}

window.onload = gestoreLoad;