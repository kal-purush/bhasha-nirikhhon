
// Generate blinking circles in header
window.addEventListener('DOMContentLoaded', () => {
  const container = document.querySelector('.circle-blink-container');
  const count = 8;
  for (let i = 0; i < count; i++) {
    const circle = document.createElement('div');
    circle.classList.add('blink-circle');
    // random position
    circle.style.left = Math.random() * 100 + '%';
    circle.style.top = Math.random() * 100 + '%';
    // random animation timing
    circle.style.animationDuration = (2 + Math.random() * 3) + 's';
    circle.style.animationDelay = Math.random() * 5 + 's';
    container.appendChild(circle);
  }
});