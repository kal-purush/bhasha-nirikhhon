// Sincronizar las letras con la canción
var audio = document.querySelector("audio");
var lyrics = document.querySelector("#lyrics");

// Array de objetos que contiene cada línea y su tiempo de aparición en segundos
var lyricsData = [
  { text: "Quiero ser tu libro favorito", time: 22 },
  { text: "Pa' que me desojes lentamente", time: 23 },
  { text: "Y siempre me mires frente a frente", time: 29 },
  { text: "Y me pongas en un rinconcito y", time: 31 },
  { text: "Me alejes la parte más bonita", time: 35 },
  { text: "Me apretujes fuerte contra ti", time: 36 },
  { text: "Que sea la razón de tu sonrisa", time: 40 },
  { text: "Contigo, yo quiero quedarme aquí contigo", time: 44 },
  { text: "Quiero que te quedes tú conmigo", time: 49 },
  { text: "Toda mi existencia te lo pido", time: 52 },
  { text: "Contigo, yo quiero quedarme aquí contigo", time: 58 },
  { text: "Quiero que me ames lo imposible", time: 60 },
  { text: "Toda mi existencia si es posible ", time: 62.5 },
  { text: "Contigoooo", time: 70 },
  { text: "Quiero que me marques con tu nombre", time: 109 },
  { text: "Tu teléfono y tu dirección", time: 110 },
  { text: "Por si un día me pierdo, me devuelvan", time: 115 },
  { text: "Derechito hasta tu corazón", time: 117 },
  { text: "Y hacerme el que nunca te había visto", time: 120 },
  { text: "Para que me leas otra vez", time: 124 },
  { text: "Propiciar de nuevo tus latidos", time: 126 },
  { text: "Contigo, yo quiero quedarme aquí contigo", time: 131 },
  { text: "Quiero que te quedes tú conmigo", time: 136 },
  { text: "Toda mi existencia te lo pido", time: 137 },
  { text: "Contigo, yo quiero quedarme aquí contigo", time: 143 },
  { text: "Quiero que me ames lo imposible", time: 148 },
  { text: "Toda mi existencia sí es posible", time: 151 },
  { text: "Y es que yo quiero ser tu libro", time: 156 },
  { text: "Para contemplar tus labios", time: 158 },
  { text: "Y la tibieza de tus manos", time:  169},
  { text: "Y amanecer acurrucados, tú y yo", time: 165 },
  { text: "Contigo, déjame quedarme aquí contigo", time: 170 },
  { text: "Quiero que te quedes tú conmigo", time:  175},
  { text: "Toda mi existencia te lo pido", time: 176 },
  { text: "Contigo, ay", time: 183 },
  { text: "Contigo, déjame quedarme aquí contigo", time: 196 },
  { text: "Quiero que te quedes tú conmigo", time:  200},
  { text: "Toda mi existencia te lo pido", time: 201 },
  { text: "Contigo, yo quiero quedarme aquí contigo", time: 208 },
  { text: "Quiero que me ames lo imposible", time: 211 },
  { text: "Toda mi existencia si es posible", time: 214},
  { text: "Contigo, ay", time: 217 },
  { text: "I lov u a ton my moon", time: 240},

];

// Animar las letras
function updateLyrics() {
  var time = Math.floor(audio.currentTime);
  var currentLine = lyricsData.find(
    (line) => time >= line.time && time < line.time + 6
  );

  if (currentLine) {
    // Calcula la opacidad basada en el tiempo en la línea actual
    var fadeInDuration = 0.1; // Duración del efecto de aparición en segundos
    var opacity = Math.min(1, (time - currentLine.time) / fadeInDuration);

    // Aplica el efecto de aparición
    lyrics.style.opacity = opacity;
    lyrics.innerHTML = currentLine.text;
  } else {
    // Restablece la opacidad y el contenido si no hay una línea actual
    lyrics.style.opacity = 0;
    lyrics.innerHTML = "";
  }
}

setInterval(updateLyrics, 1000);

//funcion titulo
// Función para ocultar el título después de 216 segundos
function ocultarTitulo() {
  var titulo = document.querySelector(".titulo");
  titulo.style.animation =
    "fadeOut 3s ease-in-out forwards"; /* Duración y función de temporización de la desaparición */
  setTimeout(function () {
    titulo.style.display = "none";
  }, 3000); // Espera 3 segundos antes de ocultar completamente
}

// Llama a la función después de 216 segundos (216,000 milisegundos)
setTimeout(ocultarTitulo, 240000);