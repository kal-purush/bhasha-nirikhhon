function busca() {
    var url = "http://www.professorisidro.com.br/tarefas.php";
    fetch(url, {"method":"GET"})
       .then(response => response.json())
       .then(json => exb(json));
}

function exb(json) {
    var meuTxt = document.getElementById("local");

    for (i=0; i<json.length; i++) {
        meuTxt.innerHTML += "<div id='" + i + "' draggable='true' ondragstart='drag(event)'>nome: " + json[i].nome + "<br/><div/>";
    }
}

function allowDrop(ev) {
    ev.preventDefault();
  }
  
  function drag(ev) {
    ev.dataTransfer.setData("text", ev.target.id);
  }
  
  function drop(ev) {
    ev.preventDefault();
    var data = ev.dataTransfer.getData("text");
    ev.target.appendChild(document.getElementById(data));

    /*var idTarefa = evt.dataTransfer.getData("text");
    var tarefa = document.getElementById(idTarefa);

    ev.target.appendChild(tarefa);*/
  }