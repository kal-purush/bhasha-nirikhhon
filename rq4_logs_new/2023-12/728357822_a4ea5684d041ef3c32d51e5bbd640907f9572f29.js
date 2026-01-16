document.addEventListener("DOMContentLoaded", function () {
  const menu = document.getElementById("mobile-and-tablet-menu-button");
  const menuItems = document.getElementById("menu-items");
  const menuText = document.getElementById("menu-text");

  menu.addEventListener("click", function () {
    menuItems.classList.toggle("appear");


  });
});
