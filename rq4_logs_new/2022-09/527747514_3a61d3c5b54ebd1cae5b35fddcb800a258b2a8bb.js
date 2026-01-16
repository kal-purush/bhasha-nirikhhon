let btnAdicionar = document.getElementById("btnAdicionar");
btnAdicionar.addEventListener("click", function () {

    let elementos = [];
    let valor = document.getElementById("valor").value;
    elementos.push(valor.toString());

    let elementoOL = document.createElement("OL");
    elementos.forEach(element => {
        let elementLI = document.createElement("LI");
        let textNode = document.createTextNode(element);
        elementLI.appendChild(textNode);

        elementoOL.appendChild(textNode);
    });

    document.body.appendChild(elementoOL);
});