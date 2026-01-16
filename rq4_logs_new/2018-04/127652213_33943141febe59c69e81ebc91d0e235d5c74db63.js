var linkJennifer = document.querySelector(".jennifer-link");
var popupJennifer = document.querySelector(".modal-pets-jennifer");
var closePopupJennifer = popupJennifer.querySelector(".modal-close");

var linkCharly = document.querySelector(".charly-link");
var popupCharly = document.querySelector(".modal-pets-charly");
var closePopupCharly = popupCharly.querySelector(".modal-close");

if (linkJennifer) {
  linkJennifer.addEventListener("click", function (evt) {
    evt.preventDefault();
    popupJennifer.classList.add("modal-pets-show");
  });

  closePopupJennifer.addEventListener("click", function (evt) {
    evt.preventDefault();
    popupJennifer.classList.remove("modal-pets-show");
  });

  window.addEventListener("keydown", function (evt) {
    if (evt.keyCode === 27) {
      evt.preventDefault();
      if (popupJennifer.classList.contains("modal-pets-show")) {
        popupJennifer.classList.remove("modal-pets-show");
      }
    }
  });
}

if (linkCharly) {
  linkCharly.addEventListener("click", function (evt) {
    evt.preventDefault();
    popupCharly.classList.add("modal-pets-show");
  });

  closePopupCharly.addEventListener("click", function (evt) {
    evt.preventDefault();
    popupCharly.classList.remove("modal-pets-show");
  });

  window.addEventListener("keydown", function (evt) {
    if (evt.keyCode === 27) {
      evt.preventDefault();
      if (popupCharly.classList.contains("modal-pets-show")) {
        popupCharly.classList.remove("modal-pets-show");
      }
    }
  });
}

