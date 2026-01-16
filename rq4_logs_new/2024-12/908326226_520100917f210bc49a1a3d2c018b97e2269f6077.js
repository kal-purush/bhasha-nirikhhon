// Variables para la calculadora normal
let currentInput = "";  // Para almacenar el valor ingresado actualmente
let previousInput = ""; // Para almacenar el valor anterior (cuando se presiona un operador)
let operator = "";      // Para almacenar el operador seleccionado

// Variables para la calculadora IMC
let peso = ""; // Variable para guardar el peso ingresado
let altura = ""; // Variable para guardar la altura ingresada
let result = ""; // Variable para guardar el resultado del cálculo
let clickCounter = 0; // Contador de clicks

// Función para alternar las secciones según el estado del switch
function toggleSections() {
  const isChecked = document.getElementById("toggleSwitch").checked;
  document.getElementById("section1").classList.toggle("active", !isChecked);
  document.getElementById("section2").classList.toggle("active", isChecked);
}

// Agregar el evento al switch
document.getElementById("toggleSwitch").addEventListener("change", toggleSections);

// Referencias a elementos del DOM
const display = document.getElementById("display"); // Display de la calculadora normal
const buttons = document.querySelectorAll(".button"); // Botones de la calculadora normal
const display2 = document.getElementById("display2"); // Display de la calculadora IMC
const instruction = document.getElementById("instruction"); // Texto dinámico de instrucciones
const buttons2 = document.querySelectorAll(".button2"); // Botones de la calculadora IMC

// Función para formatear números con separadores de miles
function formatNumber(num) {
  return Number(num).toLocaleString('en-US');
}

// Función para actualizar las instrucciones dinámicas
function updateInstructions() {
  if (clickCounter === 0) {
    instruction.textContent = "Ingresa tu peso en kilogramos y presiona 'Siguiente'.";
  } else if (clickCounter === 1) {
    instruction.textContent = "Ingresa tu altura en metros y presiona '=' para calcular el IMC.";
  } else {
    instruction.textContent = "¡Tu IMC ha sido calculado! Presiona 'C' para comenzar de nuevo.";
  }
}

// Inicializar instrucciones dinámicas
updateInstructions();

// Calculadora normal
buttons.forEach(button => {
  button.addEventListener("click", () => {
    const value = button.textContent.trim();

    if (value >= "0" && value <= "9" || value === ".") { // Si es número o punto decimal
      currentInput += value;
      display.textContent = formatNumber(currentInput);
    } else if (value === "+" || value === "-" || value === "*" || value === "/") { // Si es operador
      if (currentInput === "") return;
      previousInput = currentInput;
      operator = value;
      currentInput = "";
    } else if (value === "=") { // Si es igual
      if (currentInput === "" || previousInput === "") return;

      let result;
      switch (operator) {
        case "+":
          result = parseFloat(previousInput) + parseFloat(currentInput);
          break;
        case "-":
          result = parseFloat(previousInput) - parseFloat(currentInput);
          break;
        case "*":
          result = parseFloat(previousInput) * parseFloat(currentInput);
          break;
        case "/":
          result = parseFloat(previousInput) / parseFloat(currentInput);
          break;
        default:
          return;
      }
      display.textContent = formatNumber(result);
      currentInput = result.toString();
      previousInput = "";
      operator = "";
    } else if (value === "C") { // Limpiar
      currentInput = "";
      previousInput = "";
      operator = "";
      display.textContent = "0";
    } else if (value === "backspace") { // Retroceso
      currentInput = currentInput.slice(0, -1);
      display.textContent = currentInput === "" ? "0" : formatNumber(currentInput);
    } else if (value === "%") { // Porcentaje
      if (currentInput !== "") {
        currentInput = (parseFloat(currentInput) / 100).toString();
        display.textContent = formatNumber(currentInput);
      }
    }
  });
});

// Calculadora de IMC
buttons2.forEach(button2 => {
  button2.addEventListener("click", () => {
    const value = button2.textContent.trim();

    if (value >= "0" && value <= "9" || value === ".") { // Si es número o punto decimal
      currentInput += value;
      display2.textContent = currentInput;
    } else if (value === "C") { // Limpiar
      currentInput = "";
      peso = "";
      altura = "";
      result = "";
      clickCounter = 0; // Reiniciar el contador de clics
      display2.textContent = "0";
      updateInstructions(); // Actualizar las instrucciones
    } else if (value === "backspace") { // Retroceso
      currentInput = currentInput.slice(0, -1);
      display2.textContent = currentInput === "" ? "0" : currentInput;
    } else if (value === "arrow_forward") { // Siguiente paso
      if (clickCounter === 0) { // Guardar peso
        peso = currentInput;
        currentInput = ""; // Limpiar el input actual
        display2.textContent = "0";
        clickCounter++; // Incrementar el contador de clics
        updateInstructions(); // Actualizar las instrucciones
      }
    } else if (value === "=") { // Calcular el IMC
      if (clickCounter === 1) { // Asegurar que se ingresó peso y altura
        altura = currentInput;
        result = parseFloat(peso) / (parseFloat(altura) * parseFloat(altura));
        display2.textContent = result.toFixed(2);

        // Mostrar clasificación del IMC
        if (result < 18.5) {
          instruction.textContent = "IMC: Bajo de peso.";
        } else if (result >= 18.5 && result < 24.9) {
          instruction.textContent = "IMC: Peso normal.";
        } else if (result >= 25 && result < 29.9) {
          instruction.textContent = "IMC: Sobrepeso.";
        } else {
          instruction.textContent = "IMC: Obesidad.";
        }
        clickCounter++; // Incrementar para el estado final
      }
    }
  });
});