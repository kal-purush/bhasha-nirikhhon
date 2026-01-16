const ACCESS_TOKEN = "8182013a8276ed3dd4cf0a4894e6b035e0838481";
let shortenedLink;
const url = "https://api-ssl.bitly.com/v4/shorten";
let menu = document.getElementById("burger");
let nav = document.getElementById("nav-links");
let divShortenLinks = document.getElementById("shorten-links");
let btnShortenIt = document.getElementById("shorten-it--btn");
//let btnClearAll = document.getElementById("btnClearAll");
let form;
let arrayLinks; // deklaracja tablicy
let toShort;
let linktoShortInput = document.getElementById("linktoShortInput");
let span = document.getElementById("wrongSpan");
const longUrl =
  "https://www.frontendmentor.io/challenges/url-shortening-api-landing-page-2ce3ob-G?fbclid=IwAR0m2JwEpyfAAYOsNxo4pBwy4zQ8MbqM1BCI-Vwxb-VgYKfJquWx6eDlZdY";

function validURL(str) {
  //funkcja sprawdzajaca poprawnosc linku
  var regex =
    /(?:https?):\/\/(\w+:?\w*)?(\S+)(:\d+)?(\/|\/([\w#!:.?+=&%!\-\/]))?/;
  if (!regex.test(str)) {
    return false;
  } else {
    return true;
  }
}
function wrongLink() {
  //dodawanie komunikatu i podświetlenie na czerwono
  span.classList.add("wrong-span");
  linktoShortInput.classList.add("wrong-input");
}

menu.onclick = () => {
  nav.classList.toggle("active"); //klikalne menu ( toggle dodaje jak nie ma, usuwa jak juz jest)
};

document.addEventListener("DOMContentLoaded", () => {
  form = document.getElementById("form"); // deklaracja formularza
  form.addEventListener("submit", (event) => {
    //pobieranie danych z formularza
    event.preventDefault(); //zapobieganie przeładowaniu strony
    toShort = event.target[0].value; //pobranie danych z inputa

    if (String(toShort).length === 0) {
      //obsluga bledu jeśli puste
      span.innerText = "Please add a link";
      wrongLink();
    } else if (validURL(String(toShort)) === false) {
      span.innerText = "This is not a link. Please add a correct link";
      wrongLink();
    } else {
      span.innerText = ""; // usuwanie obsługi błędu jesli text zawiera jakiś znak
      span.classList.remove("wrong-span");
      linktoShortInput.classList.remove("wrong-input");

      fetch(url, {   //pobranie informacji z Api
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${ACCESS_TOKEN}`,
        },
        body: JSON.stringify({ long_url: toShort }),
      })
        .then((response) => response.json())
        .then((data) => {
          shortenedLink = data.link;
          console.log(shortenedLink);
          let newObj = {
            linkBefore: toShort,
            linkAfter: shortenedLink,
          };
          event.target[0].value = "";

          arrayLinks.push(newObj);
          localStorage.setItem("todoList", JSON.stringify(arrayLinks)); //zapisywanie listy do local storage i przerobienie jej na stringa

          createNewShortenBox(); //wywołanie funkcji tworzacej nowe elementy
        })
        .catch((error) => console.error(error));
    }
  });
});

const createNewShortenBox = () => {
  divShortenLinks.innerHTML = "";

  arrayLinks.forEach((arrayItem) => {
    const newDivShortenLink = document.createElement("div"); //tworzenie elementu, dodawanie klasy, i wartości z listy
    newDivShortenLink.classList.add("shorten-link");

    const newPLinktoShort = document.createElement("p");
    newPLinktoShort.innerText = arrayItem.linkBefore;

    const newPShortenLink = document.createElement("p");
    newPShortenLink.innerText = arrayItem.linkAfter;

    const newBtnCopy = document.createElement("button");
    newBtnCopy.innerText = "copy";
    newBtnCopy.onclick = () => {
      newPShortenLink.sele;
      newBtnCopy.innerText = "Copied!";
      navigator.clipboard.writeText(arrayItem.linkAfter);
      newBtnCopy.style.backgroundColor = "#3a3053"; //zmiana koloru przycisku
    };

    newDivShortenLink.append(newPLinktoShort); //dodawanie elementów do nowego diva
    newDivShortenLink.append(newPShortenLink);
    newDivShortenLink.append(newBtnCopy);
    if (document.getElementById("btnClearAll") === null) {
      const btnClearAll = document.createElement("button");
      btnClearAll.innerText = "X";
      btnClearAll.id = "btnClearAll";
      btnClearAll.classList.add("btn-round", "btn-clear");
      divShortenLinks.append(btnClearAll);
    }
    divShortenLinks.append(newDivShortenLink); //dodawanie nowego elementu div

    btnClearAll.onclick = () => {
      localStorage.clear();
      divShortenLinks.innerHTML = "";
      arrayLinks = [];
    };
  });
};

const getArrayLinks = () => {
  if (localStorage.getItem("todoList")) {
    //jesli jakis element jest juz na liscie, zwróc liste
    arrayLinks = JSON.parse(localStorage.getItem("todoList"));
    createNewShortenBox();
  } else {
    arrayLinks = []; //jesli nie zwroc pusta tablice
  }
};
getArrayLinks();

btnClearAll.onclick = () => {
  localStorage.clear();
  divShortenLinks.innerHTML = "";
  arrayLinks = [];
};