const scrolling = document.querySelector(".scrolling");

function scroller() {
  const pageHeight = window.innerHeight;
  const fullHeight = document.body.clientHeight;
  const scrolll = window.scrollY;
  const math = (scrolll / (fullHeight - pageHeight)) * 100;

  console.count(math);
  scrolling.style.width = `${math}%`;
}

window.addEventListener("scroll", scroller);