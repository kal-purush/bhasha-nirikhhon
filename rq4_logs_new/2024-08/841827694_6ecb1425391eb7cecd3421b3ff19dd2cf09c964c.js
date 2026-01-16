// Maneja el evento de clic en el botón de encriptar
document.getElementById('encriptar_boton').addEventListener('click', function() {
    // Obtiene el valor del área de texto de entrada
    const ingreso_texto = document.getElementById('ingreso_texto').value;
    // Encripta el texto obtenido
    const encryptedText = encryptText(ingreso_texto);
    // Muestra el texto encriptado en el área de texto de salida
    document.getElementById('salida_texto').value = encryptedText;
});

// Maneja el evento de clic en el botón de desencriptar
document.getElementById('desencriptarboton').addEventListener('click', function() {
    // Obtiene el valor del área de texto de entrada
    const ingreso_texto = document.getElementById('ingreso_texto').value;
    // Desencripta el texto obtenido
    const decryptedText = decryptText(ingreso_texto);
    // Muestra el texto desencriptado en el área de texto de salida
    document.getElementById('salida_texto').value = decryptedText;
});

// Maneja el evento de clic en el botón de copiar
document.getElementById('copiar_boton').addEventListener('click', function() {
    // Selecciona el área de texto de salida
    const salida_texto = document.getElementById('salida_texto');
    // Selecciona el texto en el área de texto
    salida_texto.select();
    // Para móviles, asegura que el texto está seleccionado
    salida_texto.setSelectionRange(0, 99999);
    // Copia el texto seleccionado al portapapeles
    const successful = document.execCommand('copy');
});

// Función para mostrar u ocultar el botón de copiar basado en el contenido del área de texto de salida
function toggleCopyButton() {
    const salida_texto = document.getElementById('salida_texto');
    const copiar_boton = document.getElementById('copiar_boton');
    if (salida_texto.value.trim().length > 0) {
        copiar_boton.style.display = 'block'; // Muestra el botón si hay texto en el área de salida
    } else {
        copiar_boton.style.display = 'none'; // Oculta el botón si el área de salida está vacío
    }
}

// Llama a toggleCopyButton cada vez que se actualice el área de texto de salida
document.getElementById('salida_texto').addEventListener('input', toggleCopyButton);

// Función para encriptar el texto
function encryptText(text) {
    // Define las reglas de encriptación
    const rules = {
        'e': 'enter',
        'i': 'imes',
        'a': 'ai',
        'o': 'ober',
        'u': 'ufat'
    };
    // Aplica las reglas de encriptación a cada carácter del texto
    return text.toLowerCase().split('').map(char => rules[char] || char).join('');
}

// Función para desencriptar el texto
function decryptText(text) {
    // Define las reglas de desencriptación
    const rules = {
        'enter': 'e',
        'imes': 'i',
        'ai': 'a',
        'ober': 'o',
        'ufat': 'u'
    };

    let decryptedText = text;
    // Reemplaza cada regla en orden de mayor longitud a menor para evitar sustituciones incorrectas
    Object.keys(rules).sort((a, b) => b.length - a.length).forEach(key => {
        const value = rules[key];
        const regex = new RegExp(key, 'g');
        decryptedText = decryptedText.replace(regex, value);
    });

    return decryptedText;
}

// Manejador de evento para el botón de encriptar
document.getElementById('encriptar_boton').addEventListener('click', function() {
    const ingreso_texto = document.getElementById('ingreso_texto');
    const salida_texto = document.getElementById('salida_texto');
    salida_texto.value = encryptText(ingreso_texto.value); // Encripta el texto y lo muestra en el área de salida
    ingreso_texto.value = ''; // Borra el contenido del área de texto de entrada
    toggleCopyButton(); // Actualiza la visibilidad del botón de copiar
});

// Manejador de evento para el botón de desencriptar
document.getElementById('desencriptarboton').addEventListener('click', function() {
    const ingreso_texto = document.getElementById('ingreso_texto');
    const salida_texto = document.getElementById('salida_texto');
    salida_texto.value = decryptText(ingreso_texto.value); // Desencripta el texto y lo muestra en el área de salida
    ingreso_texto.value = ''; // Borra el contenido del área de texto de entrada
    toggleCopyButton(); // Actualiza la visibilidad del botón de copiar
});

// Ejecuta cuando el contenido del documento está completamente cargado
document.addEventListener('DOMContentLoaded', () => {
    const encriptar_boton = document.getElementById('encriptar_boton');
    const desencriptar_boton = document.getElementById('desencriptarboton');
    const mensajeNoEncontrado = document.querySelector('.mensaje-no-encontrado');
    const salida_texto = document.getElementById('salida_texto');
    const ingreso_texto = document.getElementById('ingreso_texto');

    // Maneja la encriptación y actualiza la visibilidad del mensaje "No encontrado"
    function handleEncryption() {
        if (salida_texto.value.trim() !== '') {
            mensajeNoEncontrado.classList.add('hidden'); // Oculta el mensaje si hay texto en el área de salida
        } else {
            mensajeNoEncontrado.classList.remove('hidden'); // Muestra el mensaje si el área de salida está vacío
        }
    }

    // Maneja la desencriptación y actualiza la visibilidad del mensaje "No encontrado"
    function handleDecryption() {
        if (salida_texto.value.trim() !== '') {
            mensajeNoEncontrado.classList.add('hidden'); // Oculta el mensaje si hay texto en el área de salida
        } else {
            mensajeNoEncontrado.classList.remove('hidden'); // Muestra el mensaje si el área de salida está vacío
        }
    }

    encriptar_boton.addEventListener('click', handleEncryption);
    desencriptar_boton.addEventListener('click', handleDecryption);

    // Oculta el mensaje de "No encontrado" si el área de entrada está vacío
    ingreso_texto.addEventListener('input', () => {
        if (ingreso_texto.value.trim() === '') {
            mensajeNoEncontrado.classList.remove('hidden'); // Muestra el mensaje si el área de entrada está vacío
        }
    });
});

// Selecciona el área de texto de entrada
const inputTexto = document.getElementById('ingreso_texto');

// Escucha el evento de entrada de texto en el área de texto
inputTexto.addEventListener('input', function() {
    // Convierte el texto a minúsculas
    this.value = this.value.toLowerCase();
});