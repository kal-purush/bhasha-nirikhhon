window.addEventListener("load", sidenVises);

function sidenVises() {
  console.log("sidenVises");
  document.querySelector("#burgermenu").addEventListener("click", toggleMenu);
}

function toggleMenu() {
  console.log("toggleMenu");
  document.querySelector("#nav").classList.toggle("hidden");
  let skjul = document.querySelector("#nav").classList.contains("hidden");
  if (skjul == true) {
    document.querySelector("#burgermenu").textContent = "☰";
  } else {
    document.querySelector("#burgermenu").textContent = "✕";
  }
}