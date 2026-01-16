$(document).ready(function() {
  console.log('Document loaded. Adding click listeners.');

  function toggleModalDiv() {
    $("#modal-div").toggleClass("mdm-visible");
  }

  function hideNavigationBar() {
    $("#side-nav").toggleClass("drawer-visible");
  }

  function showNavigationBar() {
    $("#side-nav").toggleClass("drawer-visible");
  }

  /**
  * On click off the navigation bar - when nav bar is visble, hide the navbar
  * and toggle the modal div too.
  */
  $("#modal-div").click(function() {
    console.log('Clicked on modal div ..');
    toggleModalDiv();
    hideNavigationBar();
  });

  $("#hamburger-button").click(function() {
    console.log('Clicked on hamburger drawer button ..');
    showNavigationBar();
    toggleModalDiv();
  });

});