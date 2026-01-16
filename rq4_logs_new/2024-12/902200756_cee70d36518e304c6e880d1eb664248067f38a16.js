// Selección de elementos del DOM
const menuToggle = document.getElementById('menu-toggle');
const menuItems = document.querySelector('.nav-menu');

// Función para inicializar el estado del menú
const initializeMenu = () => {
    if (window.innerWidth < 1280) {
        menuItems.classList.add('hidden');
    } else {
        menuItems.classList.remove('hidden');
    }
};

// Evento para alternar la visibilidad del menú
menuToggle.addEventListener('click', () => {
    menuItems.classList.toggle('hidden');
});

// Optimización del evento 'resize' con debounce
let resizeTimeout;
window.addEventListener('resize', () => {
    clearTimeout(resizeTimeout);
    resizeTimeout = setTimeout(() => {
        initializeMenu();
    }, 150);
});

// Evento para cerrar el menú al hacer clic fuera de él
document.addEventListener('click', (event) => {
    const isClickOutside = !menuItems.contains(event.target) && !menuToggle.contains(event.target);
    if (isClickOutside) {
        menuItems.classList.add('hidden');
    }
});

// Efecto de fondo animado al mover el mouse
document.addEventListener('mousemove', (event) => {
    const x = event.clientX / window.innerWidth;
    const y = event.clientY / window.innerHeight;
    document.body.style.background = `rgb(${Math.floor(x * 255)}, ${Math.floor(y * 255)}, 150)`;

});

// Inicialización del diseño del menú al cargar
initializeMenu();
