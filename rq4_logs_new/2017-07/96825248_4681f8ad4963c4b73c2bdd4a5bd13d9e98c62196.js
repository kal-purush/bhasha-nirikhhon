//------------------------------------------------------
// SOLDIER
//------------------------------------------------------
function Soldier (healthArg, strengthArg) {
  this.health = healthArg;
  this.strength = strengthArg;
  // this.attack =  function () {
  //   return this.strength;
  // }
}

Soldier.prototype.attack = function () {
  return this.strength;

};

Soldier.prototype.receiveDamage = function (damage) {
  this.health -= damage;
};


//------------------------------------------------------
// VIKING
//------------------------------------------------------
function Viking (nameArg, healthArg, strengthArg) {
  Soldier.call(this, healthArg, strengthArg);
  this.name = nameArg;
}

Viking.prototype = Object.create(Soldier.prototype);
Viking.prototype.constructor = Viking;

Viking.prototype.receiveDamage = function (damage) {
  this.health -= damage;

  if (this.health > 0) {
    return this.name + " has received " + damage + " points of damage";
  }
  else {
    return this.name + " has died in act of combat";
  }
};

Viking.prototype.battleCry = function () {
  return "Odin Owns You All!";
};


//------------------------------------------------------
// SAXON
//------------------------------------------------------
function Saxon (healthArg, strengthArg) {
  Soldier.call(this, healthArg, strengthArg);
}

Saxon.prototype = Object.create(Soldier.prototype);
Saxon.prototype.constructor = Saxon;

Saxon.prototype.receiveDamage = function (damage) {
  this.health -= damage;

  if (this.health > 0) {
    return "A Saxon has received " + damage + " points of damage";
  }
  else {
    return "A Saxon has died in combat";
  }
};


//------------------------------------------------------
// WAR
//------------------------------------------------------
function War () {
  this.vikingArmy = [];
  this.saxonArmy = [];
  this.deadBodies = 0;

}

War.prototype.addViking = function (viking) {
  this.vikingArmy.push(viking);
};

War.prototype.addSaxon = function (saxon) {
  this.saxonArmy.push(saxon);
};

War.prototype.saxonAttack = function () {
  var vikingIndex = Math.floor(Math.random() * this.vikingArmy.length);
  var saxonIndex = Math.floor(Math.random() * this.saxonArmy.length);
  var theViking = this.vikingArmy[vikingIndex];
  var theSaxon = this.saxonArmy[saxonIndex];

  var result = theViking.receiveDamage(theSaxon.attack());
    var theWounded = $('.viking-box').eq(vikingIndex)

      if (theViking.health <= 0) {
        this.deadBodies +=1;
        this.vikingArmy.splice(vikingIndex, 1);
        $('.viking-box .viking-pic').eq(vikingIndex).prop('src', 'images/tombstone.jpg');
        $('.viking-box').eq(vikingIndex).addClass('death-box').removeClass('viking-box');
      }
    return result;
};

War.prototype.vikingAttack = function () {
  var vikingIndex = Math.floor(Math.random() * this.vikingArmy.length);
  var saxonIndex = Math.floor(Math.random() * this.saxonArmy.length);
  var theViking = this.vikingArmy[vikingIndex];
  var theSaxon = this.saxonArmy[saxonIndex];

  var result = theSaxon.receiveDamage(theViking.attack());
    var theWounded = $('.saxon-box').eq(saxonIndex)
  var that = this;
  if (theSaxon.health <= 0) {
    that.deadBodies +=1;
    that.saxonArmy.splice(saxonIndex, 1);
    $('.saxon-box img').eq(saxonIndex).prop('src', 'images/tombstone.jpg');
    $('.saxon-box').eq(saxonIndex).addClass('death-box').removeClass('saxon-box');
  }

  return result;
};


War.prototype.showStatus = function () {
  if (this.saxonArmy.length === 0) {
    return 'Vikings have won the war of the century!';
  }
  else if (this.vikingArmy.length === 0) {
    return 'Saxons have fought for their lives and survive another day...';
  }
  else {
    return false;
  }
};

// create the war
var theWar = new War();
// create the vikings
var viking1 = new Viking("Ragnar the Great", 241, 111);
var viking2 = new Viking("Erlocht", 75, 213);
var viking3 = new Viking("Leafe Brendanton", 225, 100);
var viking4 = new Viking("Hinsen Broskvi", 250, 50);
var viking5 = new Viking("General Kon", 180, 100);
// create the saxons
var saxon1 = new Saxon(111, 85);
var saxon2 = new Saxon(300, 50);
var saxon3 = new Saxon(50, 300);
var saxon4 = new Saxon(350, 99);
var saxon5 = new Saxon(150, 285);
// assemble the vikings into the army
theWar.addViking(viking1);
theWar.addViking(viking2);
theWar.addViking(viking3);
theWar.addViking(viking4);
theWar.addViking(viking5);
// assemble the saxons
theWar.addSaxon(saxon1);
theWar.addSaxon(saxon2);
theWar.addSaxon(saxon3);
theWar.addSaxon(saxon4);
theWar.addSaxon(saxon5);
// ---------------------------------------------------------------
// Game Logic
// function definitions

function updateDOM(){
  for(var i = 0; i < theWar.vikingArmy.length; i++){
    $('.viking-box .health').eq(i).html("<span>HEALTH:</span>"+theWar.vikingArmy[i].health);
    $('.viking-box .strength').eq(i).html("<span>STRENGTH:</span>"+theWar.vikingArmy[i].strength);
  }
  for(var i = 0; i < theWar.saxonArmy.length; i++){
    $('.saxon-box .health').eq(i).html("<span>HEALTH:</span>"+theWar.saxonArmy[i].health);
    $('.saxon-box .strength').eq(i).html("<span>STRENGTH:</span>"+theWar.saxonArmy[i].strength);
  }
  for(var i = 0; i < theWar.deadBodies; i++){
    $('.death-box .health').eq(i).html('RIP');
    $('.death-box .strength').eq(i).html('RIP');
  }

  if(theWar.showStatus()){
    $('.info').text(theWar.showStatus());
  }
}


$(document).ready(function(){
  updateDOM();


$('.viking-attack').on('click', function(){
  $('.info').text(theWar.vikingAttack());
    updateDOM();
});


$('.saxon-attack').on('click', function(){
  $('.info').text(theWar.saxonAttack());
    updateDOM();
});



});