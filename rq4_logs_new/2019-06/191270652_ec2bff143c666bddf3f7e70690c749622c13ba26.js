var toggleVisibility = function (element) {
  if (element.style.opacity === "0") {
    return element.style.opacity = "1";
  }
  return element.style.opacity = "0";
};

var hamburger = document.getElementsByClassName("hamburger")[0];

var close = document.getElementsByClassName("navbar_close-menu")[0];

hamburger.addEventListener("click", function (event) {
  var menu = document.getElementsByClassName("navbar")[0];
  menu.classList.add("open");
  //event.target.style.opacity = "0"

});

close.addEventListener("click", function (event) {
  var menu = document.getElementsByClassName("navbar")[0];
  menu.classList.remove("open");

  hamburger.style.opacity = "1";
});