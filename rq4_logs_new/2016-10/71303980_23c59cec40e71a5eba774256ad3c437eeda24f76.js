"use strict";

let $classSelected;
let $weaponSelected;
let player1 = new RobotGame.Combatants.Player();
let player2 = new RobotGame.Combatants.Player();

// randomNum between 0 - 3;
let randomNum = Math.floor(Math.random() * 3) + 0;



//****eventListeners - position selected****

 $('.class_selected').click((event) => {
    event.preventDefault();
    $classSelected = $(event.target).attr("value");
    // iterate over every function in an object to determine which class was selected
    $.each(RobotGame.Robotz, (event, index) => {
      if(event === $classSelected) {
        // index applies the current function
        player1.setClass(new index());
        // update HP based on $classSelected
        player1.health = player1.health + player1.class.healthBonus;
      }
    });
  });

//****eventListeners - position selected****

//****eventListeners - weapons selected****
  $('.weapon_selected').click((event) =>{
    event.preventDefault();
    $weaponSelected = $(event.target).attr("value");
    // iterate over every function in an object to determine which class was selected
    $.each(RobotGame.Weapons,(event, index) => {
      if(event === $weaponSelected) {
        // index applies the current function
        player1.setWeapon(new index());
        populateBattlePage();

      }
    });


  //****eventListeners - weapons selected****
// Attack button battle function

$('#attack_button').click((event) =>{
  event.preventDefault();
  player1.player1Attack(player2);
  player2.player2Attack(player1);
  populateBattlePage();
});

  // battle page logic
  function populateBattlePage() {
    $('#playerHp').html(`<div id="stat__name"><h1>${player1.playerName}</h1></div>
                      <div class="stat__font" id="stat__health"><span>You Have <strong>${player1.health}</strong> HP</span></div>
                      <div class="stat__font" id="stat__weapon"><span>Your weapon is a <strong>${player1.weapon.name}</strong></span></div>
                      <div class="stat__font" id="stat__class"><span>Your Class is <strong>${player1.class.name}</strong></span></div>`);
    $('#enemyHp').html(`<div id="stat__name"><h1>${player2.playerName}</h1></div>
                      <div class="stat__font" id="stat__health"><span>You Have <strong>${player2.health}</strong> HP</span></div>
                      <div class="stat__font" id="stat__weapon"><span>Your weapon is a <strong>${player2.weapon.name}</strong></span></div>
                      <div class="stat__font" id="stat__class"><span>Your Class is <strong>${player2.class.name}</strong></span></div>`);
  }



