// Función para inicializar el menú móvil
function initMobileMenu() {
	const mobileMenuBtn = document.getElementById('mobileMenuBtn');
	const mobileMenu = document.getElementById('mobile-menu');
	const menuIcon = document.getElementById('menuIcon');
	const closeIcon = document.getElementById('closeIcon');

	if (!mobileMenuBtn || !mobileMenu || !menuIcon || !closeIcon) return;

	function toggleMenu() {
		const isExpanded = mobileMenuBtn.getAttribute('aria-expanded') === 'true';
		
		mobileMenuBtn.setAttribute('aria-expanded', (!isExpanded).toString());
		mobileMenu.classList.toggle('translate-x-full');
		menuIcon.classList.toggle('hidden');
		closeIcon.classList.toggle('hidden');
	}

	// Event listeners
	mobileMenuBtn.addEventListener('click', toggleMenu);

	// Cerrar menú al hacer clic en los enlaces
	const menuLinks = mobileMenu.querySelectorAll('a');
	menuLinks.forEach(link => {
		link.addEventListener('click', () => {
			if (mobileMenuBtn.getAttribute('aria-expanded') === 'true') {
				toggleMenu();
			}
		});
	});
}

// Inicializar el menú en la carga inicial
document.addEventListener('DOMContentLoaded', initMobileMenu);

// Reinicializar el menú después de las transiciones de página de Astro
document.addEventListener('astro:page-load', initMobileMenu); 