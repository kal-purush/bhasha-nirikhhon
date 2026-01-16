// Take elements from DOM
const cardsEl = document.querySelector(".cards");
const favoritesNav = document.getElementById("nav__favorites");
const resultsNav = document.getElementById("nav__results");
const savedConfirmed = document.querySelector(".save-confirmed");
const savedConfirmedH1 = document.querySelector(".save-confirmed h1");
const loader = document.querySelector(".loader");

// Constant Variables
const API_KEY = "WXMGMZYxobTfenNd60R4adjA3dU2zKihucHqrcNM";
const COUNT = 10;
const API_URL = `https://api.nasa.gov/planetary/apod?api_key=${API_KEY}&count=${COUNT}`;

// Initialize Variables
let imagesFromNasa = {};
let favorites = {};

// Show content
function showContent(page) {
  if (page === "favorites") {
    favoritesNav.classList.remove("hidden");
    resultsNav.classList.add("hidden");
  } else {
    favoritesNav.classList.add("hidden");
    resultsNav.classList.remove("hidden");
  }
  window.scrollTo({ top: 0, behavior: "instant" });
  loader.classList.add("hidden");
}

// Create elements with class
function createElement(
  type,
  classArray,
  title,
  href,
  src,
  target,
  textContent
) {
  const el = document.createElement(type);

  classArray.forEach((cl) => {
    el.classList.add(cl);
  });

  if (title) {
    el.title = title;
  }

  if (href) {
    el.href = href;
  }

  if (src) {
    el.src = src;
  }

  if (target) {
    el.target = target;
  }

  if (textContent) {
    el.textContent = textContent;
  }

  return el;
}

// Create DOM Nodes
function createDOMNotes(page) {
  const obj = page === "favorites" ? favorites : imagesFromNasa;
  Object.keys(obj).forEach((key) => {
    const r = obj[key];
    // Create Card
    const card = createElement("div", ["card"]);
    // Create Anchor Element
    const anchor = createElement(
      "a",
      [],
      "View Full Image",
      r.hdurl,
      "",
      "_blank"
    );
    // Create Img
    const img = createElement(
      "img",
      ["card__image"],
      "NASA Picture of the day",
      null,
      r.url
    );
    img.loading = "lazy";
    // Create Body Container
    const cardBody = createElement("div", ["card__body"]);

    // Create Title of Img el
    const cardTitle = createElement(
      "h5",
      ["card__title"],
      null,
      null,
      null,
      null,
      r.title
    );

    // Create card clickable
    const saveCard = createElement(
      "p",
      ["clickable"],
      null,
      null,
      null,
      null,
      "Add To Favorites"
    );

    if (page === "favorites") {
      saveCard.setAttribute("onclick", `removeFromFav('${r.url}')`);
      saveCard.textContent = "Remove from favorite";
    } else {
      saveCard.setAttribute("onclick", `addCardToFav('${r.url}')`);
    }

    // Create card text
    const cardText = createElement(
      "p",
      ["card__text"],
      null,
      null,
      null,
      null,
      r.explanation
    );

    //   Create small el
    const small = createElement("small", ["card__text--muted"]);

    // Create date el
    const strong = createElement("strong", [], null, null, null, null, r.date);

    // Create copyright if existing
    let span;
    if (r.copyright) {
      span = createElement(
        "span",
        [],
        null,
        null,
        null,
        null,
        ` ${r.copyright}`
      );
    }

    // Append elements
    span ? small.append(strong, span) : small.append(strong);
    cardBody.append(cardTitle, saveCard, cardText, small);
    anchor.append(img);
    card.append(anchor, cardBody);

    cardsEl.appendChild(card);
  });
}

// Set elements to DOM
function updateDOM(page) {
  if (localStorage.getItem("nasaFav")) {
    favorites = JSON.parse(localStorage.getItem("nasaFav"));
  }
  cardsEl.textContent = "";
  createDOMNotes(page);
  showContent(page);
}

function removeFromFav(itemUrl) {
  // Check if it's already added
  if (!favorites[itemUrl] || !itemUrl) {
    return;
  }

  // Add to object
  delete favorites[itemUrl];

  // Set items to local storage
  localStorage.setItem("nasaFav", JSON.stringify(favorites));

  updateDOM("favorites");

  savedConfirmed.hidden = false;
  savedConfirmedH1.textContent = "Removed!";
  setTimeout(() => (savedConfirmed.hidden = true), 2000);
}

function addCardToFav(itemUrl) {
  // Check if it's already added
  if (favorites[itemUrl] || !itemUrl) {
    return;
  }

  // Add to object
  favorites[itemUrl] = imagesFromNasa[itemUrl];

  // Set items to local storage
  localStorage.setItem("nasaFav", JSON.stringify(favorites));

  savedConfirmed.hidden = false;
  savedConfirmedH1.textContent = "Added!";
  setTimeout(() => (savedConfirmed.hidden = true), 2000);
}

// Get 10 Images from NASA
async function getPictures() {
  loader.classList.remove("hidden");
  try {
    // Call Request and get Response
    const res = await fetch(API_URL);
    const results = await res.json();

    if (!results.error) {
      results.forEach((r) => {
        imagesFromNasa[r.url] = r;
      });
    }

    updateDOM("results");
  } catch (error) {
    // Catch Error in Here
    console.error(error);
  }
}

// On Load
// getPictures();