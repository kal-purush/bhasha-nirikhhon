
function capturaEventos(obj, evt, fn) {
	// Verifica se o objeto suporta addEventListener
	if(obj.addEventListener){
		obj.addEventListener(evt, fn, true);
	} 
	
	else {
		var evento = 'on' + evt;
		obj.attachEvent(evento, fn);
	}
}

function cancelaEvento(evt){
	// Verifica se o evento suporta stopPropagation
	if(evt.stopPropagation) {
		//  Firefox, Chrome...
		evt.stopPropagation();
		evt.preventDefault();
	} else {
		// Internet explore antigos
		evt.cancelBubble = true;
		evt.returnValue = false;
	}
}

capturaEventos(window, 'load', function(evt){
	
	// aqui pega o valor do que é inserido lá no input do html
	
		
	capturaEventos(IDenviar, 'click', function(evt){
		var xmlhttp = new XMLHttpRequest();;

		xmlhttp.onreadystatechange = function(dadosJSON){
			// se a página foi carregada de maneira correta, continua.
			if(xmlhttp.readyState === 4 && xmlhttp.status === 200) {
				// aqui pega o que escrevi no input.
				var campo = document.getElementById('campo').value.toUpperCase();
				var saida = document.getElementById('saida');
				

				var dadosJSON;
				try {
					dadosJSON = JSON.parse(xmlhttp.responseText).toUpperCase();// assim com o upercase pego tanto faz se minuscula ou maiuscula

				} catch(e) {
					eval("dadosJSON = (" + xmlhttp.responseText + ");");
				}
				
				console.log(campo);
				console.log(xmlhttp.responseText.length);
				
				var div = document.getElementById('texto');
				for( var propriedade in dadosJSON ){
					//for(var i=0;i<=xmlhttp.responseText.length;i++)
					if(campo==dadosJSON[propriedade][0].numero){
						div.innerHTML += '<br>';
						div.innerHTML += 'Número da ordem: ' + dadosJSON[propriedade][0].numero;
						div.innerHTML += '<br>';
						div.innerHTML += 'Nome do Cliente: ' + dadosJSON[propriedade][0].cliente.nome;
						div.innerHTML += '<br>';
						div.innerHTML += 'Data do Pedido: ' + dadosJSON[propriedade][0].data;
						div.innerHTML += '<br>';
						div.innerHTML += 'Valor do Pedido: R$ ' + dadosJSON[propriedade][0].valor;
						div.innerHTML += '<br>';
						div.innerHTML += 'Entregue?: ' + dadosJSON[propriedade][0].entregue;
					} else if(campo==dadosJSON[propriedade][1].numero){
						div.innerHTML += '<br>';
						div.innerHTML += 'Número da ordem: ' + dadosJSON[propriedade][1].numero;
						div.innerHTML += '<br>';
						div.innerHTML += 'Nome do Cliente: ' + dadosJSON[propriedade][1].cliente.nome;
						div.innerHTML += '<br>';
						div.innerHTML += 'Data do Pedido: ' + dadosJSON[propriedade][1].data;
						div.innerHTML += '<br>';
						div.innerHTML += 'Valor do Pedido: R$ ' + dadosJSON[propriedade][1].valor;
						div.innerHTML += '<br>';
						div.innerHTML += 'Entregue?: ' + dadosJSON[propriedade][1].entregue;
					} else if(campo==dadosJSON[propriedade][2].numero){
						div.innerHTML += '<br>';
						div.innerHTML += 'Número da ordem: ' + dadosJSON[propriedade][2].numero;
						div.innerHTML += '<br>';
						div.innerHTML += 'Nome do Cliente: ' + dadosJSON[propriedade][2].cliente.nome;
						div.innerHTML += '<br>';
						div.innerHTML += 'Data do Pedido: ' + dadosJSON[propriedade][2].data;
						div.innerHTML += '<br>';
						div.innerHTML += 'Valor do Pedido: R$ ' + dadosJSON[propriedade][2].valor;
						div.innerHTML += '<br>';
						div.innerHTML += 'Entregue?: ' + dadosJSON[propriedade][2].entregue;
					} else if(campo==dadosJSON[propriedade][3].numero){
						div.innerHTML += '<br>';
						div.innerHTML += 'Número da ordem: ' + dadosJSON[propriedade][3].numero;
						div.innerHTML += '<br>';
						div.innerHTML += 'Nome do Cliente: ' + dadosJSON[propriedade][3].cliente.nome;
						div.innerHTML += '<br>';
						div.innerHTML += 'Data do Pedido: ' + dadosJSON[propriedade][3].data;
						div.innerHTML += '<br>';
						div.innerHTML += 'Valor do Pedido: R$ ' + dadosJSON[propriedade][3].valor;
						div.innerHTML += '<br>';
						div.innerHTML += 'Entregue?: ' + dadosJSON[propriedade][3].entregue;
					} else if(campo==dadosJSON[propriedade][4].numero){
						div.innerHTML += '<br>';
						div.innerHTML += 'Número da ordem: ' + dadosJSON[propriedade][4].numero;
						div.innerHTML += '<br>';
						div.innerHTML += 'Nome do Cliente: ' + dadosJSON[propriedade][4].cliente.nome;
						div.innerHTML += '<br>';
						div.innerHTML += 'Data do Pedido: ' + dadosJSON[propriedade][4].data;
						div.innerHTML += '<br>';
						div.innerHTML += 'Valor do Pedido: R$ ' + dadosJSON[propriedade][4].valor;
						div.innerHTML += '<br>';
						div.innerHTML += 'Entregue?: ' + dadosJSON[propriedade][4].entregue;
					} else if(campo==dadosJSON[propriedade][5].numero){
						div.innerHTML += '<br>';
						div.innerHTML += 'Número da ordem: ' + dadosJSON[propriedade][5].numero;
						div.innerHTML += '<br>';
						div.innerHTML += 'Nome do Cliente: ' + dadosJSON[propriedade][5].cliente.nome;
						div.innerHTML += '<br>';
						div.innerHTML += 'Data do Pedido: ' + dadosJSON[propriedade][5].data;
						div.innerHTML += '<br>';
						div.innerHTML += 'Valor do Pedido: R$ ' + dadosJSON[propriedade][5].valor;
						div.innerHTML += '<br>';
						div.innerHTML += 'Entregue?: ' + dadosJSON[propriedade][5].entregue;
					} else if(campo==dadosJSON[propriedade][6].numero){
						div.innerHTML += '<br>';
						div.innerHTML += 'Número da ordem: ' + dadosJSON[propriedade][6].numero;
						div.innerHTML += '<br>';
						div.innerHTML += 'Nome do Cliente: ' + dadosJSON[propriedade][6].cliente.nome;
						div.innerHTML += '<br>';
						div.innerHTML += 'Data do Pedido: ' + dadosJSON[propriedade][6].data;
						div.innerHTML += '<br>';
						div.innerHTML += 'Valor do Pedido: R$ ' + dadosJSON[propriedade][6].valor;
						div.innerHTML += '<br>';
						div.innerHTML += 'Entregue?: ' + dadosJSON[propriedade][6].entregue;
					} else if(campo==dadosJSON[propriedade][7].numero){
						div.innerHTML += '<br>';
						div.innerHTML += 'Número da ordem: ' + dadosJSON[propriedade][7].numero;
						div.innerHTML += '<br>';
						div.innerHTML += 'Nome do Cliente: ' + dadosJSON[propriedade][7].cliente.nome;
						div.innerHTML += '<br>';
						div.innerHTML += 'Data do Pedido: ' + dadosJSON[propriedade][7].data;
						div.innerHTML += '<br>';
						div.innerHTML += 'Valor do Pedido: R$ ' + dadosJSON[propriedade][7].valor;
						div.innerHTML += '<br>';
						div.innerHTML += 'Entregue?: ' + dadosJSON[propriedade][7].entregue;
					} else if(campo==dadosJSON[propriedade][8].numero){
						div.innerHTML += '<br>';
						div.innerHTML += 'Número da ordem: ' + dadosJSON[propriedade][8].numero;
						div.innerHTML += '<br>';
						div.innerHTML += 'Nome do Cliente: ' + dadosJSON[propriedade][8].cliente.nome;
						div.innerHTML += '<br>';
						div.innerHTML += 'Data do Pedido: ' + dadosJSON[propriedade][8].data;
						div.innerHTML += '<br>';
						div.innerHTML += 'Valor do Pedido: R$ ' + dadosJSON[propriedade][8].valor;
						div.innerHTML += '<br>';
						div.innerHTML += 'Entregue?: ' + dadosJSON[propriedade][8].entregue;
					} else if(campo==dadosJSON[propriedade][9].numero){
						div.innerHTML += '<br>';
						div.innerHTML += 'Número da ordem: ' + dadosJSON[propriedade][9].numero;
						div.innerHTML += '<br>';
						div.innerHTML += 'Nome do Cliente: ' + dadosJSON[propriedade][9].cliente.nome;
						div.innerHTML += '<br>';
						div.innerHTML += 'Data do Pedido: ' + dadosJSON[propriedade][9].data;
						div.innerHTML += '<br>';
						div.innerHTML += 'Valor do Pedido: R$ ' + dadosJSON[propriedade][9].valor;
						div.innerHTML += '<br>';
						div.innerHTML += 'Entregue?: ' + dadosJSON[propriedade][9].entregue;
					} 



						
				}			
			}
		}
		


		// Abre a requisição com o método e url
		xmlhttp.open('GET', 'server.json', true);
		// Modifica o MimeType da requisição
		xmlhttp.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded');
		// Envia os valores
		xmlhttp.send(null);
		
		// Checagem para os IEs antigos
		var evento = evt ? evt : window.event;
		// Cancela o evento, não deixa trocar a página. mesmo que preventdefault
		cancelaEvento(evento);
	});
});




// aqui outra funcao que posso retirar depois!


function loadDoc() {
  var xmlhttp = new XMLHttpRequest();

  xmlhttp.onreadystatechange = function() {
    if (this.readyState == 4 && this.status == 200) {
    	// aqui pega o que escrevi no input.
				var campo = document.getElementById('campo').value;
				var saida = document.getElementById('saida');
				console.log(campo);

     
     var dadosJSON;
     document.getElementById("demo").innerHTML = this.responseText;
	try {
		dadosJSON = JSON.parse(xmlhttp.responseText);
		} catch(e) {
			eval("dadosJSON = (" + xmlhttp.responseText + ");");
		}
     //console.log("o que tem aqui: "+ dadosJSON[0].numero);
    }
  };

  xmlhttp.open("GET", "server.json", true);
  xmlhttp.send();

}