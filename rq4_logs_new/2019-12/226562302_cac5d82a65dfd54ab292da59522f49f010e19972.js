import { processaInstrucoes } from "./programa";

var entrada = document.getElementById("entrada");
var saida = document.getElementById("saida");
var enviar = document.getElementById("enviar");
entrada.value = "5 5\n1 2 N\nLMLMLMLMM\n3 3 E\nMMRMMRMRRM";
enviar.onclick = function() {
  saida.value = processaInstrucoes(entrada.value);
};