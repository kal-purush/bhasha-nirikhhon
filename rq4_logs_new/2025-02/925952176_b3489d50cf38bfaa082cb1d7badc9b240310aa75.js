/* Cargar el loading y luego el main */
document.addEventListener("DOMContentLoaded", function () {
    setTimeout(function () {
        const loadingScreen = document.getElementById("loading-screen");
        const mainContent = document.getElementById("main-content");

        loadingScreen.classList.add("fade-out");
        mainContent.style.display = "block";
        mainContent.classList.add("fade-in");

        loadingScreen.addEventListener("transitionend", function () {
            loadingScreen.style.display = "none";
        });
        iniciarAnimacionTexto();
    }, 1300);
});


// Función para la animación de texto
function iniciarAnimacionTexto() {
    const phrases = [
      "de Software",
      "Backend",
      "Bade de Datos",    
    ];
  
    let currentPhraseIndex = 0;
    const dynamicText = document.getElementById("dynamic-text");
  
    function typePhrase(phrase) {
      let index = 0;
      dynamicText.textContent = ""; // Limpiar el texto
  
      const typingInterval = setInterval(() => {
        if (index < phrase.length) {
          dynamicText.textContent += phrase.charAt(index);
          index++;
        } else {
          clearInterval(typingInterval);
          setTimeout(() => {
            erasePhrase();
          }, 2000); // Esperar 1 segundo antes de borrar
        }
      }, 50); // Velocidad de escritura
    }
  
    function erasePhrase() {
      let index = dynamicText.textContent.length;
  
      const erasingInterval = setInterval(() => {
        if (index > 0) {
          dynamicText.textContent = dynamicText.textContent.slice(0, index - 1);
          index--;
        } else {
          clearInterval(erasingInterval);
          currentPhraseIndex = (currentPhraseIndex + 1) % phrases.length; // Cambiar a la siguiente frase
          setTimeout(() => {
            typePhrase(phrases[currentPhraseIndex]);
          }, 500); // Esperar medio segundo antes de escribir la siguiente frase
        }
      }, 50); // Velocidad de borrado
    }
    // Iniciar la animación
    typePhrase(phrases[currentPhraseIndex]);
  }