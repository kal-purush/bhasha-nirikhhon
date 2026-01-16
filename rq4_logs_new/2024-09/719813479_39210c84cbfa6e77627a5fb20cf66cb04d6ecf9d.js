let interval = 1000;

function contarSegundos() {
    setInterval(() => {
        // Envia uma mensagem pro "script.js" avisando que 1 segundo foi passado:
        postMessage("tick");
    }, interval);
}

// Inicia o Temporizador
contarSegundos();