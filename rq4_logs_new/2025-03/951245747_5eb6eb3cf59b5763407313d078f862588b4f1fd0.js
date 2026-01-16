document.getElementById('imprimirActividad').addEventListener('click', function() {
    // Obtener el parámetro de la URL actual
    const urlParams = new URLSearchParams(window.location.search);
    const ev = urlParams.get('ev');

    // Construir la nueva URL
    const newUrl = `${window.location.origin}/actividad_imprimir.php?ev=${ev}`;

    // Cambiar la URL sin recargar la página
    window.history.pushState({ path: newUrl }, '', newUrl);

    // Opcional: Puedes agregar aquí una llamada AJAX para obtener datos adicionales
    // o ejecutar alguna lógica después de cambiar la URL.

    console.log(`URL cambiada a: ${newUrl}`);
});