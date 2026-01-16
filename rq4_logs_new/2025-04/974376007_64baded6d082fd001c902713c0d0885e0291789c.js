const vidasJugador = document.getElementById('vidasJugador');
const vidasMaquina = document.getElementById('vidasMaquina');
const alerta = document.getElementById('alerta');
const eleccionJugador = document.getElementById('eleccionJugador');
const eleccionMaquina = document.getElementById('eleccionMaquina');
const resultadoRonda = document.getElementById('resultadoRonda');
const resultadoCombate = document.getElementById('resultadoCombate');

let vidasJ = 3;
let vidasM = 3;

function actualizarVidas() {
    vidasJugador.textContent = "Jugador:" + "♥️".repeat(vidasJ);
    vidasMaquina.textContent = "Máquina:" + "♥️".repeat(vidasM);
}

const elecciones = ['piedra', 'papel', 'tijera'];

function eleccionesMaquina() {
    let eleccionMaq = Math.floor(Math.random() * 3)
    return elecciones[eleccionMaq]
}

function combate(eleccionJ) {
    let eleccionM = eleccionesMaquina()
    resultadoRonda.textContent = '';

    eleccionJugador.src = `./assets/images/${eleccionJ}.png`
    eleccionMaquina.src = `./assets/images/${eleccionM}.png`
    if (eleccionJ === eleccionM) {
        resultadoRonda.textContent = 'Ronda empatada 🫱🏻‍🫲🏻'
    } else if (
        (eleccionJ === 'piedra' && eleccionM === 'tijera') ||
        (eleccionJ === 'papel' && eleccionM === 'piedra') ||
        (eleccionJ === 'tijera' && eleccionM === 'papel')
    ) {
        resultadoRonda.textContent = '¡Haz ganado esta ronda 👍🏻!'
        vidasM--
    } else {
        resultadoRonda.textContent = '¡Haz perdido esta ronda 👎🏻!'
        vidasJ--
    }
    actualizarVidas()
    finCombate()
}


function obtenerMensaje(vidasJ, vidasM) {
    if (vidasJ === 0) {
        return 'Haz perdido el combate 😣'
    } if (vidasM === 0) {
        return 'Haz ganado el combate 🎉'
    }
    return ''
}
function finCombate() {
    if (vidasJ === 0 || vidasM === 0) {
        resultadoRonda.textContent = '';
        resultadoCombate.innerText = obtenerMensaje(vidasJ, vidasM)
        alerta.textContent = 'Fin del combate ⚔️'

        if (vidasJ === 0) {
            resultadoCombate.style.color = 'red'
        }
        if (vidasM === 0) {
            resultadoCombate.style.color = 'green'
        }
        desactivarBotones()
    }
}

function desactivarBotones() {
    const botones = document.querySelectorAll('.btnOpciones')
    botones.forEach(boton => {
        boton.disabled = true
        boton.style.pointerEvents = 'none'
        boton.style.opacity = '0.3'
    })
    document.getElementById('btnReiniciar').style.display = 'block'
}

function activarBotones() {
    const botones = document.querySelectorAll('.btnOpciones')
    botones.forEach(boton => {
        boton.disabled = false
        boton.style.pointerEvents = 'auto'
        boton.style.opacity = '1'
    })
    document.getElementById('btnReiniciar').style.display='none'
}

function reiniciarJuego() {
    alerta.textContent = 'Reiniciando...🔄'
    activarBotones()
    setTimeout(() => {
        vidasJ = 3
        vidasM = 3
        actualizarVidas()
        activarBotones()
        eleccionJugador.src = ''
        eleccionMaquina.src = ''
        resultadoRonda.textContent = ''
        resultadoCombate.textContent = ''
        alerta.textContent = '¡Haz tu elección!'
    }, 1000);
}

actualizarVidas()