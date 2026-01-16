//slider items| array.from
var sliderImages = Array.from(
  document.querySelectorAll(".slider-container img")
);
var lenth = sliderImages.length;

//Set current slide
var currentSlide = 1;

//slide number element
var sliderNumber = document.getElementById("slide-number");
var nextButton = document.getElementById("next");
var prevButton = document.getElementById("prev");

nextButton.onclick = nextSlide;
prevButton.onclick = prevSlide;

//creat Ul Element
let pag = document.createElement("ul");
pag.id = "pag-ul";

for (let i = 1; i <= lenth; i++) {
  let pagitem = document.createElement("li");
  pagitem.setAttribute("data-index", i);
  pagitem.appendChild(document.createTextNode(i));
  pag.appendChild(pagitem);
}
document.getElementById("indicators").appendChild(pag);

var pagElement = document.getElementById("pag-ul");

var pagBullets = Array.from(document.querySelectorAll("#pag-ul li"));

//loop through all Bullets
for (let i = 0; i < pagBullets.length; i++) {
  pagBullets[i].onclick = function () {
    currentSlide = parseInt(this.getAttribute("data-index"));
    theChecker();
  };
}

theChecker();
//next slider function
function nextSlide() {
  if (nextButton.classList.contains("disabled")) {
    return false;
  } else {
    currentSlide++;
    theChecker();
  }
}

function prevSlide() {
  if (prevButton.classList.contains("disabled")) {
    return false;
  } else {
    currentSlide--;
    theChecker();
  }
}

function theChecker() {
  sliderNumber.textContent = "Slide | " + currentSlide;
  removAllActive();

  //set active class
  sliderImages[currentSlide - 1].classList.add("active");
  //set active pag
  pag.children[currentSlide - 1].classList.add("active");

  if (currentSlide == 1) {
    //add disablel on prev
    prevButton.classList.add("disabled");
  } else {
    prevButton.classList.remove("disabled");
  }
  if (currentSlide == lenth) {
    //add disablel on prev
    nextButton.classList.add("disabled");
  } else {
    nextButton.classList.remove("disabled");
  }
}

// remove all active classes
function removAllActive() {
  sliderImages.forEach(function (img) {
    img.classList.remove("active");
  });
  pagBullets.forEach(function (bullet) {
    bullet.classList.remove("active");
  });
}