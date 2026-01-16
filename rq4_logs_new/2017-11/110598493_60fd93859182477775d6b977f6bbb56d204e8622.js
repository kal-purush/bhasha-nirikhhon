function verificar() {
    var n1 = document.getElementById("n1").innerHTML; // pega o contéuda da div
    var n2 = document.getElementById("n2").value;     // pega o campo do input

    if(n1 == n2) {
        alert("VOCÊ ACERTOU O NÚMERO!");
    } else {
        alert("VOCÊ ERROU!");
    }

    resetar();
}
function resetar() {
    document.getElementById("n2").value = "";

    var aleatorio = Math.floor(Math.random() * 10);
    document.getElementById("n1").innerHTML = aleatorio;
}

