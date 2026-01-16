// JavaScript Document

$(document).ready(function(){
	selectFromDb('inventory','*','editdate','DESC');
	
});
	
function selectFromDb(table,cols,keycol,order){
	var type_array = new Array();
	
	var dataString="table="+table+"&cols="+cols+"&keycol="+keycol+"&order="+order+"&action=select";
	//alert(dataString);
	
	$.ajax({
		type: "POST",
		url:"http://pakhuis.hosts.ma-cloud.nl/medialab/php/getdata.php",
		data: dataString,
		xhrFields: { withCredentials: true },
		crossDomain: true,
		cache: false,
		success: function(data){
		//alert(data);
		if(data == 'geen data'){
			alert("geen data");
			}else{
				var result = JSON.parse(data);	
				$.each(result, function(i, field){
					var id=field.id;
					var name=field.name;
					var type=field.type;
					var state=field.state;
					type_array.push(type);
					
					$("#itemlist").append("<li id='"+id+"' class='"+type+" "+state+"'><h1>"+name+" ("+id+")</h1>"+type+"</li>");
					});
				}
			}, 
			complete: function(data){
					//alert("ik ben klaar");
					type_array = jQuery.unique(type_array);
					generateTypes(type_array);
					}
		});
}

function generateTypes(type_array){
	$.each( type_array, function( key, value ) {
		//alert( key + ": " + value );
		$("#types").append("<input id='"+value+"' type='checkbox' checked><label for='"+value+"'>"+value+"</label>");
		});
}