function traerCurso(){
	
	$.post("Carreras?accion=cursos&carrera="+$("#select_carrera").val(), function( data ) {
		
		
		$("#container_cursos").html(data);
		
		
		
	});
	
	
	
}

function emitirReporte(){
	
		if($("#select_curso").val()!=null && $("#select_curso").val()!=""){
			var win = window.open("redirect.jsp", "",
					"height=600,width=900,resizable=yes,scrollbars=yes");
			win.title = "Reporte Lecturas";
			win.document.write('<form id="frmparam" action="pepe" method="post" name="frmparam" ></form>');
		
			var url = "Reportes?numr=1&id_curso="+$("#select_curso").val();
			var frm = win.document.getElementById("frmparam");
			frm.action = encodeURI(url);
			//wwin.document.forms[0].submit();
			frm.submit();
		}else{
			
			var error = {
					
					cd_error : 0,
					ds_error : "Debe seleccionar un curso",
					tipo:"error"
					
					
			}
			
			lanzarMensaje(error);

			
			
		}

	
}