const header = document.querySelector('header');
const updateClipPath = (e) => {
  const rect = header.getBoundingClientRect();
  const x = e.clientX - rect.left;
  const y = e.clientY - rect.top;
  header.style.setProperty('--clip', `circle(55px at ${x}px ${y}px)`);
};
header.addEventListener('mousemove', updateClipPath);
header.addEventListener('mouseenter', updateClipPath);
header.addEventListener('mouseleave', () => {
  header.style.setProperty('--clip', `circle(0px at 50% 50%)`);
});
 
document.addEventListener('DOMContentLoaded', () => {
  const animatableElements = document.querySelectorAll('body *:not(.glow):not(script):not(style):not(.glowmobile)');
  animatableElements.forEach(el => el.classList.add('animate'));
  const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        entry.target.classList.add('in-view');
      } else {
        entry.target.classList.remove('in-view');
      }
    });
  }, { threshold: 0.1 });
  animatableElements.forEach(el => observer.observe(el));
});
    