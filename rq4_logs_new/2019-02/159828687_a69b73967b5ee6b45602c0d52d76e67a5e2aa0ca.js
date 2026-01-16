function navbar() {
  var x = document.getElementById("navbar-top");
  if (x.className === "navbar") {
    x.className += " resp-navbar";
  } else {
    x.className = "navbar";
  }
}
function changeDisplay(el, angleSelect) {
  e = document.getElementById(el);
  a = document.getElementById(angleSelect);
  if (e.style.visibility == "hidden") {
    e.style.visibility = "visible";
    e.style.height = "50px";
    a.style.transform = "rotate(180deg)";
  } else {
    e.style.visibility = "hidden";
    e.style.height = "0";
    a.style.transform = "rotate(0deg)";
  }
}