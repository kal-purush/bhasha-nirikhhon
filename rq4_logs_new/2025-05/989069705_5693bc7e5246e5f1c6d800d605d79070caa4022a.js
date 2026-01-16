// Références aux éléments HTML
const ecranAccueil = document.getElementById("ecran-accueil");
const boutonDemarrer = document.getElementById("btn-demarrer");

const zoneDeJeu = document.getElementById("jeu");
const infos = document.getElementById("infos");

const affichageTemps = document.getElementById("temps");
const affichageScore = document.getElementById("score");

const ecranFin = document.getElementById("ecran-fin");
const scoreFinal = document.getElementById("score-final");
const boutonRejouer = document.getElementById("btn-rejouer");

let score = 0;
let tempsRestant = 30;
let intervalleAlien;
let intervalleTemps;

// Fonction pour créer un alien à une position aléatoire
function creerAlien() {
  const alien = document.createElement("div");
  alien.classList.add("alien");

  const maxX = zoneDeJeu.clientWidth - 50;
  const maxY = zoneDeJeu.clientHeight - 50;

  alien.style.left = Math.random() * maxX + "px";
  alien.style.top = Math.random() * maxY + "px";

  alien.onclick = () => {
    score++;
    affichageScore.textContent = score;
    alien.remove();
  };

  zoneDeJeu.appendChild(alien);

  setTimeout(() => alien.remove(), 1200);
}

// Fonction pour démarrer le jeu
function demarrerJeu() {
  // Réinitialisation
  score = 0;
  tempsRestant = 30;
  affichageScore.textContent = "0";
  affichageTemps.textContent = "30";
  zoneDeJeu.innerHTML = "";

  // Gérer la visibilité des écrans
  ecranAccueil.classList.remove("visible");
  ecranFin.classList.remove("visible");
  zoneDeJeu.classList.remove("cache");
  infos.classList.remove("cache");

  intervalleAlien = setInterval(creerAlien, 800);

  intervalleTemps = setInterval(() => {
    tempsRestant--;
    affichageTemps.textContent = tempsRestant;

    if (tempsRestant <= 0) {
      terminerJeu();
    }
  }, 1000);
}

// Fonction pour terminer le jeu
function terminerJeu() {
  clearInterval(intervalleAlien);
  clearInterval(intervalleTemps);

  zoneDeJeu.classList.add("cache");
  infos.classList.add("cache");

  scoreFinal.textContent = score;

  ecranFin.classList.add("visible");
}

// Événements de démarrage et redémarrage
boutonDemarrer.onclick = demarrerJeu;
boutonRejouer.onclick = demarrerJeu;