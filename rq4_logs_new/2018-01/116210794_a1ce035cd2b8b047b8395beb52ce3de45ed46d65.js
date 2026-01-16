'use strict';

var nameVar = 'Vadim';
var nameVar = 'Mikke';
console.log('nameVar', nameVar);

var nameLet = 'Jen';
nameLet = 'july';
console.log('nameLet', nameLet);

var nameConst = 'frank';
console.log('nameConst', nameConst);

function getPetName() {
  var petName = 'Hal';
  return petName;
}

var petName = getPetName();
console.log(petName);