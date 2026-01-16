const hamburgerBtn = document.querySelector(".toggle-nav");

// Menu rideau
const left = document.querySelector(".left");
const right = document.querySelector(".right");

// Hovering elements
var marque = document.querySelector(".marque");
var pecherie = document.querySelector(".pecherie");
var produit = document.querySelector(".produit");
var valeur = document.querySelector(".valeur");
var recrutement = document.querySelector(".recrutement");
var others = document.querySelectorAll(".other");

// sub-nav desktop to show
const subNavDesktopMarque = document.querySelector(".sub-nav.desktop.marque");
const subNavDesktopPecherie = document.querySelector(
  ".sub-nav.desktop.pecherie"
);
const subNavDesktopProduit = document.querySelector(".sub-nav.desktop.produit");
const subNavDesktopValeur = document.querySelector(".sub-nav.desktop.valeur");
const subNavDesktopRecrutement = document.querySelector(
  ".sub-nav.desktop.recrutement"
);

// sub-nav desktop to show
const subNavMobileMarque = document.querySelector(".sub-nav.mobile.marque");
const subNavMobilePecherie = document.querySelector(".sub-nav.mobile.pecherie");
const subNavMobileProduit = document.querySelector(".sub-nav.mobile.produit");
const subNavMobileValeur = document.querySelector(".sub-nav.mobile.valeur");
const subNavMobileRecrutement = document.querySelector(
  ".sub-nav.mobile.recrutement"
);

// clicking sub-nav desktop item
const items = document.querySelectorAll(".sub-nav.desktop a");

// clicking sub-nav mobile item
const itemsMobile = document.querySelectorAll(".sub-nav.mobile a");

// removing active
const allItemsNav = document.querySelectorAll(".left ul li a");

hamburgerBtn.addEventListener("click", toggleNav);

// toggle nav for Menu button
function toggleNav() {
  hamburgerBtn.classList.toggle("active");
  left.classList.toggle("active");
  right.classList.toggle("active");
  //   DESKTOP
  subNavDesktopMarque.classList.remove("active");
  subNavDesktopPecherie.classList.remove("active");
  subNavDesktopProduit.classList.remove("active");
  subNavDesktopValeur.classList.remove("active");
  subNavDesktopRecrutement.classList.remove("active");
  // MOBILE
  subNavMobileMarque.classList.remove("active");
  subNavMobilePecherie.classList.remove("active");
  subNavMobileProduit.classList.remove("active");
  subNavMobileValeur.classList.remove("active");
  subNavMobileRecrutement.classList.remove("active");
}

// Activing sub-nav
// FOR DESKTOP
marque.addEventListener("mouseover", function () {
  // DESKTOP
  subNavDesktopMarque.classList.add("active");
  subNavDesktopPecherie.classList.remove("active");
  subNavDesktopProduit.classList.remove("active");
  subNavDesktopValeur.classList.remove("active");
  subNavDesktopRecrutement.classList.remove("active");
});
pecherie.addEventListener("mouseover", function () {
  // DESKTOP
  subNavDesktopPecherie.classList.add("active");
  subNavDesktopMarque.classList.remove("active");
  subNavDesktopProduit.classList.remove("active");
  subNavDesktopValeur.classList.remove("active");
  subNavDesktopRecrutement.classList.remove("active");
});
produit.addEventListener("mouseover", function () {
  // DESKTOP
  subNavDesktopProduit.classList.add("active");
  subNavDesktopPecherie.classList.remove("active");
  subNavDesktopMarque.classList.remove("active");
  subNavDesktopValeur.classList.remove("active");
  subNavDesktopRecrutement.classList.remove("active");
});
valeur.addEventListener("mouseover", function () {
  // DESKTOP
  subNavDesktopValeur.classList.add("active");
  subNavDesktopProduit.classList.remove("active");
  subNavDesktopPecherie.classList.remove("active");
  subNavDesktopMarque.classList.remove("active");
  subNavDesktopRecrutement.classList.remove("active");
});
recrutement.addEventListener("mouseover", function () {
  // DESKTOP
  subNavDesktopRecrutement.classList.add("active");
  subNavDesktopProduit.classList.remove("active");
  subNavDesktopPecherie.classList.remove("active");
  subNavDesktopMarque.classList.remove("active");
  subNavDesktopValeur.classList.remove("active");
});

others.forEach(function (other) {
  other.addEventListener("mouseover", function () {
    // DESKTOP
    subNavDesktopMarque.classList.remove("active");
    subNavDesktopPecherie.classList.remove("active");
    subNavDesktopProduit.classList.remove("active");
    subNavDesktopValeur.classList.remove("active");
    subNavDesktopRecrutement.classList.remove("active");
  });
});

// FOR MOBILE
marque.addEventListener("click", function () {
  // MOBILE
  subNavMobileMarque.classList.add("active");
  subNavMobilePecherie.classList.remove("active");
  subNavMobileProduit.classList.remove("active");
  subNavMobileValeur.classList.remove("active");
  subNavMobileRecrutement.classList.remove("active");
});
pecherie.addEventListener("click", function () {
  // MOBILE
  subNavMobilePecherie.classList.add("active");
  subNavMobileMarque.classList.remove("active");
  subNavMobileProduit.classList.remove("active");
  subNavMobileValeur.classList.remove("active");
  subNavMobileRecrutement.classList.remove("active");
});
produit.addEventListener("click", function () {
  // MOBILE
  subNavMobileProduit.classList.add("active");
  subNavMobileMarque.classList.remove("active");
  subNavMobilePecherie.classList.remove("active");
  subNavMobileValeur.classList.remove("active");
  subNavMobileRecrutement.classList.remove("active");
});
valeur.addEventListener("click", function () {
  // MOBILE
  subNavMobileValeur.classList.add("active");
  subNavMobileMarque.classList.remove("active");
  subNavMobilePecherie.classList.remove("active");
  subNavMobileProduit.classList.remove("active");
  subNavMobileRecrutement.classList.remove("active");
});
recrutement.addEventListener("click", function () {
  // MOBILE
  subNavMobileRecrutement.classList.add("active");
  subNavMobileMarque.classList.remove("active");
  subNavMobilePecherie.classList.remove("active");
  subNavMobileProduit.classList.remove("active");
  subNavMobileValeur.classList.remove("active");
});

others.forEach(function (other) {
  other.addEventListener("click", function () {
    // MOBILE
    subNavMobileMarque.classList.remove("active");
    subNavMobilePecherie.classList.remove("active");
    subNavMobileProduit.classList.remove("active");
    subNavMobileValeur.classList.remove("active");
    subNavMobileRecrutement.classList.remove("active");
  });
});

// clicking to remove current sub-nav desktop
items.forEach(function (element) {
  element.addEventListener("click", function () {
    subNavDesktopMarque.classList.remove("active");
    subNavDesktopPecherie.classList.remove("active");
    subNavDesktopProduit.classList.remove("active");
    subNavDesktopValeur.classList.remove("active");
    subNavDesktopRecrutement.classList.remove("active");
  });
});

// clicking to remove current sub-nav mobile
itemsMobile.forEach(function (element) {
  element.addEventListener("click", function () {
    subNavMobileMarque.classList.remove("active");
    subNavMobileMarque.classList.remove("active");
    subNavMobilePecherie.classList.remove("active");
    subNavMobileProduit.classList.remove("active");
    subNavMobileValeur.classList.remove("active");
    subNavMobileRecrutement.classList.remove("active");
  });
});

// clicking to remove all active sub-nav
allItemsNav.forEach(function (element) {
  element.addEventListener("click", function () {
    // DESKTOP
    subNavDesktopMarque.classList.remove("active");
    subNavDesktopPecherie.classList.remove("active");
    subNavDesktopProduit.classList.remove("active");
    subNavDesktopValeur.classList.remove("active");
    subNavDesktopRecrutement.classList.remove("active");
    // MOBILE
    subNavMobileMarque.classList.remove("active");
    subNavMobilePecherie.classList.remove("active");
    subNavMobileProduit.classList.remove("active");
    subNavMobileValeur.classList.remove("active");
    subNavMobileRecrutement.classList.remove("active");
  });
});