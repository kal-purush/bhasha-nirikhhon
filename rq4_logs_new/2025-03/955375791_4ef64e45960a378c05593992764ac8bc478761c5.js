// Funciones de validación
function formatRut(value) {
    // Eliminar puntos y guiones
    let rut = value.replace(/[.-]/g, '');
    
    // Separar cuerpo y dígito verificador
    const body = rut.slice(0, -1);
    const dv = rut.slice(-1).toUpperCase();
    
    // Formatear el cuerpo con puntos
    let formattedBody = '';
    for (let i = body.length - 1; i >= 0; i--) {
        formattedBody = body[i] + formattedBody;
        if ((body.length - i) % 3 === 0 && i !== 0) {
            formattedBody = '.' + formattedBody;
        }
    }
    
    // Devolver RUT formateado
    return formattedBody + '-' + dv;
}

function formatPhone(value) {
    // Eliminar todos los caracteres no numéricos
    return value.replace(/\D/g, '');
}

function validateRut(rut) {
    // Eliminar puntos y guiones
    const cleanRut = rut.replace(/[.-]/g, '');
    
    // Verificar longitud mínima
    if (cleanRut.length < 2) return false;
    
    // Separar cuerpo y dígito verificador
    const body = cleanRut.slice(0, -1);
    const dv = cleanRut.slice(-1).toUpperCase();
    
    // Calcular dígito verificador esperado
    let sum = 0;
    let multiplier = 2;
    
    for (let i = body.length - 1; i >= 0; i--) {
        sum += parseInt(body[i]) * multiplier;
        multiplier = multiplier === 7 ? 2 : multiplier + 1;
    }
    
    const expectedDV = 11 - (sum % 11);
    const calculatedDV = expectedDV === 11 ? '0' : expectedDV === 10 ? 'K' : expectedDV.toString();
    
    // Comparar dígito verificador calculado con el proporcionado
    return calculatedDV === dv;
}

function validateChileanPhone(phone) {
    // Verificar que empiece con 9 y tenga 9 dígitos
    return /^9\d{8}$/.test(phone);
}

// Manejo del formulario
document.addEventListener('DOMContentLoaded', function() {
    const contactForm = document.getElementById('contactForm');
    const phoneInput = document.getElementById('phone');
    const rutInput = document.getElementById('rut');
    const phoneError = document.getElementById('phone-error');
    const rutError = document.getElementById('rut-error');
    
    // Formatear RUT mientras se escribe
    rutInput.addEventListener('input', function(e) {
        const cleanValue = e.target.value.replace(/[.-]/g, '');
        
        if (cleanValue.length > 1) {
            e.target.value = formatRut(cleanValue);
        } else {
            e.target.value = cleanValue;
        }
    });
    
    // Formatear teléfono mientras se escribe
    phoneInput.addEventListener('input', function(e) {
        e.target.value = formatPhone(e.target.value);
    });
    
    // Validación al enviar el formulario
    contactForm.addEventListener('submit', function(e) {
        e.preventDefault();
        
        // Validar teléfono
        const phone = phoneInput.value;
        if (!validateChileanPhone(phone)) {
            phoneError.textContent = 'Por favor ingresa un número de teléfono válido (9 dígitos, comenzando con 9)';
            return;
        } else {
            phoneError.textContent = '';
        }
        
        // Validar RUT
        const rut = rutInput.value;
        if (!validateRut(rut)) {
            rutError.textContent = 'Por favor ingresa un RUT válido con el formato XX.XXX.XXX-X';
            return;
        } else {
            rutError.textContent = '';
        }
        
        // Si todo es válido, enviar el formulario
        // En una implementación real, aquí iría el código para enviar los datos al servidor
        console.log('Formulario enviado con éxito', { phone, rut });
        
        // Redirigir a la página de agradecimiento
        window.location.href = 'gracias.html';
    });
});