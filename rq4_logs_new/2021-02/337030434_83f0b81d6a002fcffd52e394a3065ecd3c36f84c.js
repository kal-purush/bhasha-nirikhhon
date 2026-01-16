const lineX = document.querySelector('.lineX');
const lineY = document.querySelector('.lineY');
const target = document.querySelector('.target');
const coordinate = document.querySelector('.coordinate');

window.addEventListener('mousemove', (e) => {
  const x = e.clientX;
  const y = e.clientY;

  lineX.style.top = `${y}px`
  lineY.style.left = `${x}px`

  target.style.top = `${y}px`
  target.style.left = `${x}px`

  coordinate.style.top = `${y}px`
  coordinate.style.left = `${x}px`
  coordinate.innerHTML = `${x}px, ${y}px`
});