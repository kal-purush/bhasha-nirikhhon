'use strict';

function typeOfGroup(musicians) {
  if (musicians.length <= 0) {
    console.log(`not a group`);
  } else if (musicians.length === 1) {
    console.log(`Solo`);
  } else if (musicians.length === 2) {
    console.log(`Duet`);
  } else if (musicians.length === 3) {
    console.log(`Trio`);
  } else if (musicians.length === 4) {
    console.log(`Quartet`);
  } else if (musicians.length > 4) {
    console.log(`This is a large group`);
  } else {
    console.log(`Invalid Entry`);
  }
}

typeOfGroup([`Taiye`, `Kehinde`, `Alaba`, `Idowu`, `keke`]);