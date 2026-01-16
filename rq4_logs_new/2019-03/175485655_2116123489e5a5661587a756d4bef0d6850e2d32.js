$(document).ready(function () {
  $(".product__form__button .button").on("click", function () {
    $('.modal').toggleClass('modal--show');
    $('body').toggleClass('no-scroll');
  });

  $(".modal__buttons .button").on("click", function () {
    $('body').toggleClass('no-scroll');
    $('.modal').toggleClass('modal--show');
  });
});