// Smooth Scrolling
const projectLinks = document.querySelectorAll('a[href^="#"]');
projectLinks.forEach(link => {
	link.addEventListener('click', function(event) {
		event.preventDefault(); // Prevent default jump behavior
		const targetId = this.getAttribute('href');
		const targetElement = document.querySelector(targetId);
		// Smooth scroll to the target element
		targetElement.scrollIntoView({
			behavior: 'smooth' 
		});
	});
});
// Optional: Add Hover Animations
const projectItems = document.querySelectorAll('li');
projectItems.forEach(item => {
	item.addEventListener('mouseover', () => {
		item.style.boxShadow = '0 4px 10px rgba(0, 0, 0, 0.2)'; // Add hover shadow
	});
	item.addEventListener('mouseout', () => {
		item.style.boxShadow = '0 2px 5px rgb(192, 255, 184, 0.1)'; // Reset shadow
	});
});