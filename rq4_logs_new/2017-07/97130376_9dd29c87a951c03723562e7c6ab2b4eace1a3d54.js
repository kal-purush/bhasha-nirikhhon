$(document).ready(function(){

var attacker;
var benHealth = 120;
var benAttack = 10;
var landoHealth = 100;
var landoAttack = 20;
var R2Health = 200;
var R2Attack = 30;
var reyHealth = 300;
var reyAttack = 40;

function benGame(){
  attack = 5;
  defense = 120;


};


$('.ben').on("click",function(){
  $('.lando').appendTo(".char").addClass("col-lg-3 col-md-3 col-sm-12 col-xs-12");
  $('.R2D2').appendTo(".char").addClass("col-lg-3 col-md-3 col-sm-12 col-xs-12");
  $('.rey').appendTo(".char").addClass("col-lg-3 col-md-3 col-sm-12 col-xs-12");
  benGame();

  $(".lando").on("click", function() {
  	benHealth -= 20;
  	$('#benHealth').html(benHealth);
  	console.log(benHealth);
  });
});




});