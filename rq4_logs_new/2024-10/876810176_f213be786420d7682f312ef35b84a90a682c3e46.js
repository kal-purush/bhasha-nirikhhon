// Selección de elementos de una sola vez
const sidebar = document.querySelector('.sidebar');
const overlay = document.querySelector('.overlay');
const body = document.body;

// Reutilizar función para togglear clases
function toggleClass(element, className) {
  if (element) {
    element.classList.toggle(className);
  }
}

// Reutilizar función para remover clases
function removeClass(element, className) {
  if (element) {
    element.classList.remove(className);
  }
}

// Mostrar/Ocultar sidebar
document.getElementById('menuToggle')?.addEventListener('click', () => {
  toggleClass(sidebar, 'show');
  toggleClass(overlay, 'show');
  toggleClass(body, 'sidebar-active');
});

// Cerrar la barra lateral cuando se hace clic en el overlay
overlay?.addEventListener('click', () => {
  removeClass(sidebar, 'show');
  removeClass(overlay, 'show');
  removeClass(body, 'sidebar-active');
});

// Ajustar la visibilidad del menú desplegable del perfil
document.getElementById('dropdownButton')?.addEventListener('click', (e) => {
  e.stopPropagation(); // Evitar que el evento se propague y cierre el menú
  toggleClass(document.getElementById('dropdownMenu'), 'show');
});

// Toggle para mostrar y ocultar el menú desplegable de Mantenimiento
document.getElementById('maintenanceDropdown')?.addEventListener('click', (e) => {
  e.preventDefault(); // Evita que el enlace recargue la página
  toggleClass(document.getElementById('maintenanceMenu'), 'show');
});

// Cerrar menús desplegables si se hace clic fuera de ellos
window.addEventListener('click', (e) => {
  if (!e.target.closest('#dropdownButton') && !e.target.closest('.dropdown')) {
    removeClass(document.getElementById('dropdownMenu'), 'show');
  }
  if (!e.target.closest('#maintenanceDropdown') && !e.target.closest('#maintenanceMenu')) {
    removeClass(document.getElementById('maintenanceMenu'), 'show');
  }
});

// Toggle para mostrar y ocultar el campo de búsqueda
document.getElementById('searchToggle')?.addEventListener('click', () => {
  const searchBar = document.querySelector('.search-bar');
  toggleClass(searchBar, 'active');
  if (searchBar.classList.contains('active')) {
    searchBar.querySelector('input').focus();
  }
});

// Asegurarse de cerrar el sidebar y ajustar el contenido en pantallas más grandes
window.addEventListener('resize', () => {
  if (window.innerWidth > 768) {
    removeClass(sidebar, 'show');
    removeClass(overlay, 'show');
    removeClass(body, 'sidebar-active');
  }
});

console.log("Script cargado correctamente");