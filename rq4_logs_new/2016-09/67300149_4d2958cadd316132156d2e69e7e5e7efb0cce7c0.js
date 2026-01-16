<!--
	var idcaja = 0;
	var im;
	var imagenes = new Array("cargando.gif", "guardar.jpg", "eliminar.jpg", "interfaz_fondo.jpg", "menu_bar.jpg", "bar_menu2.jpg", "bizqsup.png", "bsup.jpg", "bdersup.png", "bizq.jpg", "bder.jpg", "bizqinf.png", "binf.jpg", "bderinf.png");
	var lista_imagenes = new Array();
	for(im in imagenes){
		lista_imagenes[im] = new Image();
		lista_imagenes[im].src = "imagenes/"+imagenes[im];
	}
	
	dias = [31,29,31,30,31,30,31,31,30,31,30,31];
	meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"];
	
	function diasDeMes(mes, anio){
		ultimo = 0;
		if (mes == 1){
			fecha = new Date(anio,1,29);
			vermes = fecha.getMonth();
			if(vermes != mes){
				ultimo = 28;
			}
		}
		if(ultimo == 0){
			ultimo = dias[mes];
		}
		return ultimo;
	}
	
	$(document).ready(function (){
//		document.body.onre
		CambioCuenta();
		$("#imgEliminar").click(function (){
			if(idcaja != 0){
				if(confirm('¿Está Seguro de Eliminar?')){
					Eliminar(idcaja);
				}
			}
			else{
				alert('Debe seleccionar un registro.');
			}
		});
		$("#imgRenovar").click(function(){
			actualiza();
			actualizaCuotas();
		})
/*		document.getElementById("imgRenovar").onclick = function(){
			moverListas(document.estilos.estilo.value);
		}*/
		
		$("#estilo").bind("change", function(){
			cambiarEstilo('estilos'+$("#estilo").value);
			moverListas($("#estilo").value);
		});
		
		var fecha = new Date();
		for(i = 1; i <= diasDeMes(fecha.getMonth(),fecha.getFullYear()); i++){
			op = document.createElement("OPTION");
			op.value = i; 
			op.text = i;
			$("#dia").append(op);
		}
		$("#dia").val(fecha.getDate());
		for(i = 0; i < 12; i++){
			op = document.createElement("OPTION");
			op.value = i + 1; 
			op.text = meses[i];
			$("#mes").append(op);
		}
		$("#mes").val(fecha.getMonth()+1);
		
		var cont = 0;
		for(i = 2015; i < 2020; i++){
			op = document.createElement("OPTION");
			op.value = i; 
			op.text = i;
			$("#anio").append(op);
			cont++;
		}
		$("#anio").val(fecha.getFullYear());
		
		for(i = 0; i < 12; i++){
			op = document.createElement("OPTION");
			op.value = i + 1; 
			op.text = meses[i];
			$("#mes").append(op);
			$("#filmes").append(op);
		}
		
		var cont = 1;
		for(i = 2015; i < 2020; i++){
			op = document.createElement("OPTION");
			op.value = i; 
			op.text = i;
			$("#anio").append(op);
			$("#filanio").append(op);
			cont++;
		}
		
		$("#cuenta").bind("change", CambioCuenta);
		$("#monto").bind("keypress", function(){
			//return(numericodecimal(this));
		});

		$("#imgGuardar").click(function(){
			var descripcion = $("#descripcion");
			var monto = $("#monto");
			if(descripcion.value == ''){
				alert('Debe Comletar la Descripcion');
				descripcion.focus();
			}
			else if(monto.value == ''){
				alert('Debe Completar el Monto');
				monto.focus();
			}
			else{
				var cboDia = $("#dia").val();
				var cboMes = $("#mes").val();
				var cboAnio = $("#anio").val();
				var cuenta = $("#cuenta").val();
				var tarjeta = $("#tarjeta").val();
				var cuotas = $("#cuotas").val();
				var tipo = $("#tipo").val();
				var user1 = $("#user1").val();
				cargaDatos(cboDia, cboMes, cboAnio, descripcion.val(), cuenta, tarjeta, cuotas, tipo, monto.val(), user1);
			}
		});
		
		$("#filmes").bind("change", actualiza);
		$("#filanio").bind("change", actualiza);
		$("#filuser").bind("change", actualiza);
		$("#filcuenta").bind("change", actualiza);

		actualiza();
		actualizaCuotas();
		res = screen.width;
		$("#estilo").val(res);
		if(res >= 1280){
			res2 = 1280;
		}
		else{
			res2 = res;
		}
		cambiarEstilo('estilos'+res2);
		moverListas(res2);
		ajustar();
	});
	
	/***************************************/
	/*****  Funcion para Mover Capas  ******/
	/***************************************
	function Browser() {
	
	  var ua, s, i;
	
	  this.isIE    = false;
	  this.isNS    = false;
	  this.version = null;
	
	  ua = navigator.userAgent;
	
	  s = "MSIE";
	  if ((i = ua.indexOf(s)) >= 0) {
	    this.isIE = true;
	    this.version = parseFloat(ua.substr(i + s.length));
	    return;
	  }
	
	  s = "Netscape6/";
	  if ((i = ua.indexOf(s)) >= 0) {
	    this.isNS = true;
	    this.version = parseFloat(ua.substr(i + s.length));
	    return;
	  }
	
	  // Treat any other "Gecko" browser as NS 6.1.
	
	  s = "Gecko";
	  if ((i = ua.indexOf(s)) >= 0) {
	    this.isNS = true;
	    this.version = 6.1;
	    return;
	  }
	}
	
	var browser = new Browser();
	
	var dragObj = new Object();
	dragObj.zIndex = 0;
	
	function dragStart(event, id) {
	
	  var el;
	  var x, y;
	
	  // If an element id was given, find it. Otherwise use the element being
	  // clicked on.
	
	  if (id)
	    dragObj.elNode = $(id);
	  else {
	    if (browser.isIE)
	      dragObj.elNode = window.event.srcElement;
	    if (browser.isNS)
	      dragObj.elNode = event.target;
	
	    // If this is a text node, use its parent element.
	
	    if (dragObj.elNode.nodeType == 3)
	      dragObj.elNode = dragObj.elNode.parentNode;
	  }
	
	  // Get cursor position with respect to the page.
	
	  if (browser.isIE) {
	    x = window.event.clientX + document.documentElement.scrollLeft
	      + document.body.scrollLeft;
	    y = window.event.clientY + document.documentElement.scrollTop
	      + document.body.scrollTop;
	  }
	  if (browser.isNS) {
	    x = event.clientX + window.scrollX;
	    y = event.clientY + window.scrollY;
	  }
	
	  // Save starting positions of cursor and element.
	
	  dragObj.cursorStartX = x;
	  dragObj.cursorStartY = y;
	  dragObj.elStartLeft  = parseInt(dragObj.elNode.style.left, 10);
	  dragObj.elStartTop   = parseInt(dragObj.elNode.style.top,  10);
	
	  if (isNaN(dragObj.elStartLeft)) dragObj.elStartLeft = 0;
	  if (isNaN(dragObj.elStartTop))  dragObj.elStartTop  = 0;
	
	  // Update element's z-index.
	
	  dragObj.elNode.style.zIndex = ++dragObj.zIndex;
	
	  // Capture mousemove and mouseup events on the page.
	
	  if (browser.isIE) {
	    document.attachEvent("onmousemove", dragGo);
	    document.attachEvent("onmouseup",	dragStop);
	    window.event.cancelBubble = true;
	    window.event.returnValue = false;
	  }
	  if (browser.isNS) {
	    document.addEventListener("mousemove", dragGo,   true);
	    document.addEventListener("mouseup",   dragStop, true);
	    event.preventDefault();
	  }
	}

	function dragGo(event) {
	
	  var x, y;
	
	  // Get cursor position with respect to the page.
	
	  if (browser.isIE) {
	    x = window.event.clientX + document.documentElement.scrollLeft
	      + document.body.scrollLeft;
	    y = window.event.clientY + document.documentElement.scrollTop
	      + document.body.scrollTop;
	  }
	  if (browser.isNS) {
	    x = event.clientX + window.scrollX;
	    y = event.clientY + window.scrollY;
	  }
	
	  // Move drag element by the same amount the cursor has moved.
	
	  dragObj.elNode.style.left = (dragObj.elStartLeft + x - dragObj.cursorStartX) + "px";
	  dragObj.elNode.style.top  = (dragObj.elStartTop  + y - dragObj.cursorStartY) + "px";
	
	  if (browser.isIE) {
	    window.event.cancelBubble = true;
	    window.event.returnValue = false;
	  }
	  if (browser.isNS)
	    event.preventDefault();
	}
	
	function dragStop(event) {
	
	  // Stop capturing mousemove and mouseup events.
	
	  if (browser.isIE) {
	    document.detachEvent("onmousemove", dragGo);
	    document.detachEvent("onmouseup",	dragStop);
	  }
	  if (browser.isNS) {
	    document.removeEventListener("mousemove", dragGo,	true);
	    document.removeEventListener("mouseup",   dragStop, true);
	  }
	}*/
	
	function moverListas(res){
		var ListaAct = $("#DListaAct");
		var ListaGas = $("#DListaGas");
		var ListaAcu = $("#DCuotasAcu");
	
		if(res==800){
			ListaAct.css("top",95);
			ListaAct.css("left",308);
			ListaGas.css("top",182);
			ListaGas.css("left",308);
			ListaAcu.css("top",100);
			ListaAcu.css("left",550);
		}
		else if(res==1024){
			ListaAct.css("top",99);
			ListaAct.css("left",730);
			ListaGas.css("top",200);
			ListaGas.css("left",730);
			ListaAcu.css("top",340);
			ListaAcu.css("left",730);
		}
		else if(res>=1280){
			ListaAct.css("top",100);
			ListaAct.css("left",800);
			ListaGas.css("top",207);
			ListaGas.css("left",800);
			ListaAcu.css("top",370);
			ListaAcu.css("left",800);
		}
	}
	/***********************************/
	/************   FIN   **************/
	/***********************************/
	
	/*------- Funcion en Ajax ---------*/
	function nuevoAjax(){ 
		/* Crea el objeto AJAX. Esta funcion es generica para cualquier utilidad de este tipo, por lo que se puede copiar tal como esta aqui */
		var xmlhttp=false; 
		try{ 
			// Creacion del objeto AJAX para navegadores no IE
			xmlhttp=new ActiveXObject("Msxml2.XMLHTTP"); 
		}
		catch(e){
			try{
				// Creacion del objet AJAX para IE
				xmlhttp=new ActiveXObject("Microsoft.XMLHTTP");
			}
			catch(E) { xmlhttp=false; }
		}
		if (!xmlhttp && typeof XMLHttpRequest!='undefined') { xmlhttp=new XMLHttpRequest(); } 

		return xmlhttp; 
	} 
	/*--------- Actualizar Listas ---------*/
	function actualiza(){
		$('#info').html("Seleccione un registro");
		actualiza_Lista();
		actualiza_Actual();
		actualiza_Gastos();
		idcaja=0;
}
	function actualizaCuotas(){
		actualiza_Cuotas();
		actualiza_CuotasAcu();
		idcaja=0;
}
	/*--------- Ingresa los Datos a la Base ---------*/
	function cargaDatos(p_dia, p_mes, p_anio, p_descripcion, p_cuenta, p_tarjeta, p_cuotas, p_tipo, p_monto, p_user1){
		$.ajax({
			url: "ingreso_sin_recargar_proceso.php",
			method: "POST",
			data: { 
				dia:p_dia, mes:p_mes, anio:p_anio, 
				descripcion:p_descripcion, 
				cuenta:p_cuenta, 
				tarjeta:p_tarjeta,
				cuotas:p_cuotas, tipo:p_tipo,
				monto:p_monto, idadmin:p_user1 
			}
		});
		$("#descripcion").val("");
		$("#monto").val("");
		actualiza();
		actualizaCuotas();
	}
	/*--------- Elimina un Registro ---------*/
	function Eliminar(id){
		ajax1=nuevoAjax();
		ajax1.open("GET", "eliminar_registro.php?idcaja="+id, true);
		ajax1.send(null);
		actualiza();
		actualizaCuotas();
		idcaja=0;
	}
	/*--------- Actualizar Lista de Detalle -----------*/
	function actualiza_Lista(){
		var lista1=$("#Lista");
		lista1.html("Actualizando<img src='imagenes/cargando.gif' border='0'>");
		$.ajax({
			url: "actualiza_lista.php",
			method: "POST",
			data: { 
				messel:$("#filmes").val(),
				anosel:$("#filanio").val(),
				usersel:$("#filuser").val(),
				cuentasel:$("#filcuenta").val()
			},
			success: function(text){
				lista1.html(text);
			}
		});
	}
	/*--------- Actualizar Tabla de Actual -----------*/
	function actualiza_Actual(){
		var listaAct=$("#ListaAct");
		listaAct.html("Actualizando<img src='imagenes/cargando.gif' border='0'>");
		$.ajax({
			url: "actualiza_actual.php",
			method: "POST",
			success: function(text){
				listaAct.html(text);
			}
		});
	}
	/*--------- Actualizar Tabla de Gastos -----------*/
	function actualiza_Gastos(){
		var listaGas=$("#ListaGas");
		listaGas.html("Actualizando<img src='imagenes/cargando.gif' border='0'>");
		$.ajax({
			url: "actualiza_gastos.php",
			method: "POST",
			data: { 
				messel:$("#filmes").val(),
				anosel:$("#filanio").val()
			},
			success: function(text){
				listaGas.html(text);
			}
		});
	}
	/*--------- Actualizar Tabla de Cuotas -----------*/
	function actualiza_Cuotas(){
		var listaC=$("#ListaC");
		listaC.html("Actualizando<img src='imagenes/cargando.gif' border='0'>");
		$.ajax({
			url: "actualiza_cuotas.php",
			method: "POST",
			success: function(text){
				listaC.html(text);
			}
		});
	}
	/*--------- Actualizar Tabla de Cuotas -----------*/
	function actualiza_CuotasAcu(){
		var CuotasAcu=$("#CuotasAcu");
		CuotasAcu.html("Actualizando<img src='imagenes/cargando.gif' border='0'>");
		$.ajax({
			url: "actualiza_cuotas_acumuladas.php",
			method: "POST",
			success: function(text){
				CuotasAcu.html(text);
			}
		});
	}
	/*--------- Text Numerico ---------*/
	function numericodecimal(text, e)
	{
		var strCheck = '0123456789.';
		var whichCode = (window.Event) ? e.which : e.keyCode; 
		if(whichCode == 13) return true;
		key = String.fromCharCode(whichCode); // Get key value from key code
		if(strCheck.indexOf(key) == -1) return false; // Not a valid key
	}
	/*--------------------------------*/
	/*------- Text Alfavetica --------*/
	function letras(text, e)
	{
		var strCheck = 'QWERTYUIOPASDFGHJKLÑZXCVBNMqwertyuiopasdfghjklñzxcvbnm';
		var whichCode = (window.Event) ? e.which : e.keyCode; 
		if(whichCode == 13) return true;
		key = String.fromCharCode(whichCode); // Get key value from key code
		if(strCheck.indexOf(key) == -1) return false; // Not a valid key
	}
	/*--------------------------------*/
	/*-- Text Numerica con Formato ---*/
	function currencyFormat(fld, milSep, decSep, e)
	{
		var sep = 0;
		var key = '';
		var i = j = 0;
		var len = len2 = 0;
		var strCheck = '0123456789';
		var aux = aux2 = '';
		var whichCode = (window.Event) ? e.which : e.keyCode;
		if(whichCode == 13) return true;
		key = String.fromCharCode(whichCode); // Get key value from key code
		if(strCheck.indexOf(key) == -1) return false; // Not a valid key
		len = fld.value.length;
		for(i = 0; i < len; i++)
			if ((fld.value.charAt(i) != '0') && (fld.value.charAt(i) != decSep)) break;
			aux = '';
			for(; i < len; i++)
			if (strCheck.indexOf(fld.value.charAt(i))!=-1) aux += fld.value.charAt(i);
			aux += key;
			len = aux.length;
			if (len == 0) fld.value = '';
			if (len == 1) fld.value = '0'+ decSep + '0' + aux;
			if (len == 2) fld.value = '0'+ decSep + aux;
			if (len > 2)
			{
				aux2 = ''; 
				for (j = 0, i = len - 3; i >= 0; i--)
				{
					if (j == 3)
					{
						aux2 += milSep;
						j = 0;
					}
					aux2 += aux.charAt(i);
					j++;
				}
			fld.value = '';
			len2 = aux2.length;
			for (i = len2 - 1; i >= 0; i--)
				fld.value += aux2.charAt(i);
				fld.value += decSep + aux.substr(len - 2, len);
			}
		return false;
	}
	function Accion(accion, id)
	{
		if(accion=="eliminar")
		{
			var conf = confirm('¿Está seguro que desea eliminar?')
			if(!conf)
			{
				return false;
			}
		}
		if(accion=="aceptar")
		{
			var conf = confirm('¿Está seguro que desea modificar?')
			if(!conf)
			{
				return false;
			}
		}
		if(id)
		{
			document.getElementById("id").value = id;
		}
		document.getElementById("opcion").value = accion;
		//document.caja.submit();
	}
	/*--------------------------------*/
	function Color(color, fila)
	{
		$("#"+fila).css("backgroundColor", color);
	}
	/*--------------------------------*/
	function CambioCuenta()
	{
		if($("#cuenta").val() == 3){
			$("#Dtarjeta").show();
			$("#Dcuotas").show();
			$("#Dtipo").hide();
			$("#Ddescripcion").show();
		}
		else if($("#cuenta").val() == 5 || $("#cuenta").val() == 6){
			$("#Dtarjeta").hide();
			$("#Dcuotas").hide();
			$("#Dtipo").hide();
			$("#Ddescripcion").hide();
			if($("#cuenta").val() == 5){
				$("#descripcion").val('Ahorro');
			}
			else{
				$("#descripcion").val('Extraccion');
			}
		}
		else{
			$("#Dtarjeta").hide();
			$("#Dcuotas").hide();
			$("#Dtipo").show();
			$("#Ddescripcion").show();
		}
	}
	function cambiarEstilo(clase){
		var cssNode = document.createElement('link');
		cssNode.type = 'text/css';
		cssNode.rel = 'stylesheet';
		cssNode.href = clase + ".css";
		cssNode.media = 'screen';
		cssNode.title = 'dynamicLoadedSheet';
		document.getElementsByTagName("head")[0].appendChild(cssNode);
	}
	function ajustar(){
		$("#scroll").css("height", document.documentElement.clientHeight-92 +"px");
		$("#scroll2").css("height", document.documentElement.clientHeight-110 +"px");
		$("#DListaC").css("height", document.documentElement.clientHeight-110 +"px");
	}
	function MostrarOcultar(Tabla){
		if(Tabla.display == ""){
			Tabla.display = "none";
		}
		else{
			Tabla.display = "";
		}
	}
//-->