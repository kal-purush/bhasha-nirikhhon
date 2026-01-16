function encriptarTexto(texto) {
  const reglas = {
    'e': 'enter',
    'i': 'imes',
    'a': 'ai',
    'o': 'ober',
    'u': 'ufat'
  };

  return texto.replace(/[eioua]/g, (letra) => reglas[letra]);
}

function desencriptarTexto(texto) {
  const reglas = {
    'enter': 'e',
    'imes': 'i',
    'ai': 'a',
    'ober': 'o',
    'ufat': 'u'
  };

  return texto.replace(/enter|imes|ai|ober|ufat/g, (palabra) => reglas[palabra]);
}

function copiarTexto() {
  const textarea = document.getElementById('texto-derecha');
  const copiarBtn = document.getElementById('copiar-btn');
  if (textarea.value.trim() === "") {
    textarea.classList.add('hidden');
    copiarBtn.classList.add('hidden');
  } else {
    textarea.select();
    navigator.clipboard.writeText(textarea.value)
      .then(() => {
        console.log('Texto copiado al portapapeles');
      })
      .catch(err => {
        console.error('Error al copiar el texto: ', err);
      });
  }
}

document.addEventListener('DOMContentLoaded', () => {
  const textareaIzquierda = document.getElementById('texto-izquierda');
  const placeholderImage = document.getElementById('placeholder-image');
  const textareaDerecha = document.getElementById('texto-derecha');
  const copiarBtn = document.getElementById('copiar-btn');
  const encriptarBtn = document.querySelector('button[type="submit"]');
  const desencriptarBtn = document.querySelector('button[formaction="desencriptar.php"]');

  textareaIzquierda.addEventListener('input', () => {
    if (textareaIzquierda.value.trim() !== "") {
      placeholderImage.classList.add('hidden');
      textareaDerecha.classList.remove('hidden');
      copiarBtn.classList.remove('hidden');
    } else {
      placeholderImage.classList.remove('hidden');
      textareaDerecha.classList.add('hidden');
      copiarBtn.classList.add('hidden');
    }
  });

  encriptarBtn.addEventListener('click', (event) => {
    event.preventDefault();
    const texto = textareaIzquierda.value;
    const textoEncriptado = encriptarTexto(texto);
    textareaDerecha.value = textoEncriptado;
    textareaDerecha.classList.remove('hidden');
  });

  desencriptarBtn.addEventListener('click', (event) => {
    event.preventDefault();
    const texto = textareaIzquierda.value;
    const textoDesencriptado = desencriptarTexto(texto);
    textareaDerecha.value = textoDesencriptado;
    textareaDerecha.classList.remove('hidden');
  });
});