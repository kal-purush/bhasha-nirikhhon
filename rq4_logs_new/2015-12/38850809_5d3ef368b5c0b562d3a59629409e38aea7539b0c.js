function barrel_roll() {
  $('body').addClass('barrel_roll');
  setTimeout("$('body').removeClass('barrel_roll')", 4000);
  console.log("roll");
}