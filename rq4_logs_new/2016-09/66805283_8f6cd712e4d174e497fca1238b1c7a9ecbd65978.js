function modalFunctions() {
  $('.modal-trigger').leanModal({
    opacity: .7
  });
  $(".button-collapse").sideNav();
  Materialize.toast($toastContent, 5000, $toastClass);
  $('.cart').material_chip();
  $('.dropdown-button').dropdown();
}

$(document).on('turbolinks:load', modalFunctions)