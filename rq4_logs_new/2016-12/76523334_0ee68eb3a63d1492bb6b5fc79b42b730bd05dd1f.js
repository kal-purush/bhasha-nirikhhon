var inicioPares = function ()
{
	var arreglo = [];
	for (var i = 1; i <= 16; i++) {
			arreglo [i-1] = i;
		}
	alert(arreglo[1]);
//	function mezclar(arreglo){
//   		var m= arreglo.length-1;
//  		for (var i=m;i>1;i--){ 
//    	  alea=Math.floor(i*Math.random()); 
//    	  temp=arreglo[i]; 
//    	   arreglo[i]=arreglo[alea]; 
//    	   arreglo[alea]=temp; 
//   		 }
//  	 return(arreglo);
//  	 alert (arreglo[i]);
//	}

	var destapa = function ()
	{
		//var n = Math.floor((Math.random()*16)+1);
		//alert (n);
		$("#CartaBocaAbajo1").hide("slow");
		$("#CartaBocaArriba1").show("slow");
	}

	$("#CartaBocaAbajo1").on("click",destapa);
}
$(document).on("ready",inicioPares);