function carregarUsuario(){
    var usuarioLogado = localStorage.getItem("logado");

    if (!usuarioLogado){
        window.location="index.html";
    }else{
        var usuarioJson = JSON.parse(usuarioLogado);
        document.getElementById("foto").innerHTML = 
            "<img alt='Imagem não encontrada' width='20%' src=images/" + usuarioJson.foto + ">";
        document.getElementById("nome").innerHTML = 
            "<h5>" + usuarioJson.racf + " - " + usuarioJson.email + "</h5>"
    }
}


function gerar(){

    var data = document.getElementById("txtData").value;
    var ano = data.substring(0,4);
    var mes = data.substring(5,7);
    var dia = data.substring(8);
    var dataFormatada = dia + "/" + mes + "/" + ano;
  
    var agencia  = document.getElementById("txtAgencia").value.length;
    var dataAgenda  = document.getElementById("txtData").value.length;
    var cliente  = document.getElementById("txtCliente").value.length;

    window.alert(agencia);
    window.alert(dataAgenda);
    window.alert(cliente);

    if (agencia>0){
        agencia = 1
    }
    if (dataAgenda>0){
        dataAgenda = 3
    }
    if (cliente>0){
        cliente = 5
    }
    var resultado = agencia + dataAgenda + cliente;

    var mensagem = {
              agencia : {
                    id : document.getElementById("txtAgencia").value
                }
    }


   window.alert(resultado);

   //window.alert(JSON.stringify(mensagem));

   var cabecalho = {
       method:"POST",
       body:JSON.stringify(mensagem),
       headers:{
            "Content-Type":"Application/json"
       }
   }

    fetch("http://localhost:8080/relatorioporagencia", cabecalho)
    .then(res => res.json())
    .then(res => listarAgendamentos(res));
}

function listarAgendamentos(lista){    
    var saida = 
    "<div class='row>" + 
    "<div class='col-12>" +
    "<table border='1' cellpadding='5' cellspacing='2' align='center' width'80%'>" +
    "<tr>" + 
    "<th>Cliente</th> <th>Email</th> <th>Celular</th> <th>Data Agendamento</th> <th>Hora Agendamento</th>" + 
    "</tr>";
    for(i=0;i<lista.length;i++){
        saida+=
        "<tr>" +
        "<td>" + lista[i].nomeCli + "</td>" +
        "<td>" + lista[i].email + "</td>" +
        "<td>" + lista[i].celular + "</td>" +
        "<td>" + lista[i].dataAgendamento + "</td>" +
        "<td>" + lista[i].horaAgendamento + "</td>" +
        "</tr>";
    }
    saida+="</table></div></div>"
    document.getElementById("agendamento").innerHTML=saida;

}


