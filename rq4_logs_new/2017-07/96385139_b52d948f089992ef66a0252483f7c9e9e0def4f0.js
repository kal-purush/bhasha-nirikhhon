/**
 * Created by pablo on 7/8/17.
 */


var count = 0;
var vencedor = 0;
function clickTeste(id) {
    count++;
    //verificarVencedor(id);

    var quadrado = document.getElementById(id);

    //console.log(document.getElementById(id).value);

    if (vencedor !== 1) {

        if (count % 2 === 0) {
            quadrado.innerHTML = "X";
            document.getElementById(id).value = "X";
        } else {
            quadrado.innerHTML = "O";
            document.getElementById(id).value = "O";
        }
    }


    verificarVencedor(id);


}


function verificarVencedor(id) {
    var quadrados = document.getElementsByTagName("td");

    if (vencedor !== 1) {
        if (quadrados[0].value === "X" && quadrados[1].value === "X" && quadrados[2].value === "X" ||
            quadrados[3].value === "X" && quadrados[4].value === "X" && quadrados[5].value === "X" ||
            quadrados[6].value === "X" && quadrados[7].value === "X" && quadrados[8].value === "X" ||
            quadrados[0].value === "X" && quadrados[3].value === "X" && quadrados[6].value === "X" ||
            quadrados[1].value === "X" && quadrados[4].value === "X" && quadrados[7].value === "X" ||
            quadrados[2].value === "X" && quadrados[5].value === "X" && quadrados[8].value === "X" ||
            quadrados[0].value === "X" && quadrados[6].value === "X" && quadrados[8].value === "X" ||
            quadrados[3].value === "X" && quadrados[5].value === "X" && quadrados[6].value === "X") {
            alert("Vencedor jogador X");
            vencedor++;
        } else if (quadrados[0].value === "O" && quadrados[1].value === "O" && quadrados[2].value === "O" ||
            quadrados[3].value === "O" && quadrados[4].value === "O" && quadrados[5].value === "O" ||
            quadrados[6].value === "O" && quadrados[7].value === "O" && quadrados[8].value === "O" ||
            quadrados[0].value === "O" && quadrados[3].value === "O" && quadrados[6].value === "O" ||
            quadrados[1].value === "O" && quadrados[4].value === "O" && quadrados[7].value === "O" ||
            quadrados[2].value === "O" && quadrados[5].value === "O" && quadrados[8].value === "O" ||
            quadrados[0].value === "O" && quadrados[4].value === "O" && quadrados[8].value === "O" ||
            quadrados[3].value === "O" && quadrados[5].value === "O" && quadrados[6].value === "O") {
            alert("Vencedor jogador O");
            vencedor++;
        } else if (count === 9) {
            alert("Nenhum Vencedor");
        }
    }
}