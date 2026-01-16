// Sélectionner les éléments HTML à modifier
const about = document.querySelector("#about");
const work = document.querySelector("#work");
const contact = document.querySelector("#contact");

// Ajouter un événement de défilement à la fenêtre
window.addEventListener("scroll", () => {
  // Vérifier la hauteur de la page au scroll et modifier les classes des éléments en conséquence
  const scrollTop = document.documentElement.scrollTop;
  if (scrollTop <= 1100) {
    about.classList.add("active");
    work.classList.remove("active");
    contact.classList.remove("active");
  } else if (scrollTop <= 2000) {
    about.classList.remove("active");
    work.classList.add("active");
    contact.classList.remove("active");
  } else {
    about.classList.remove("active");
    work.classList.remove("active");
    contact.classList.add("active");
  }
});