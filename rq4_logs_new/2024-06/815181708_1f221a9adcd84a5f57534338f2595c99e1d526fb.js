import { verificarTema, trocarTema } from "../../helpers/tema-helpers.js"

const botaoTema = document.querySelector(".tema button")
const body = document.querySelector("body")  

botaoTema.addEventListener("click", () => { 
    trocarTema(body, botaoTema)
}) 

verificarTema(body, botaoTema)