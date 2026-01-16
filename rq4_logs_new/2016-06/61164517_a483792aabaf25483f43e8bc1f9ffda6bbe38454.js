//------------------------------------------------------------------------------------------------------------------
// YOUR CODE: Create your Station and Bike "constructor" function and "prototypes" methods here.
//------------------------------------------------------------------------------------------------------------------




//------------------------------------------------------------------------------------------------------------------
// DRIVER CODE: Do **NOT** change anything below this point. Your task is to implement code above to make this work.
//------------------------------------------------------------------------------------------------------------------

function assert(test, message) {
  if (!test) {
    throw "ERROR: " + message;
  }
  return true;
}

var bikes = [
  new Bike(1237, "red", 2011),
  new Bike(4465, "blue", 2014),
  new Bike(1234, "red", 2011),
  new Bike(3485, "blue", 2014),
  new Bike(9485, "blue", 2015),
  new Bike(8348, "blue", 2015),
  new Bike(3485, "blue", 2014),
];

station.replenish(bikes);

assert(
  station.name === "Divvy", "the station has a name"
);
assert(
  station.location === "Wood & Division", "the station is located on Wood and Division"
);
assert(
  station.selectByColor("red").length === 2, "the station should have 2 red bikes"
);
assert(
  station.mostRecent().length === 5, "the station has 5 'new' bikes (since 2014)"
  // the mostRecent function should return all bikes built after 2014
);
assert(
  bikes[0].id === 1237, "expected 1237"
);
assert(
  bikes[2].color === "red", "expected 'red'"
);
assert(
  bikes[4].brandNew() === true, "expected 'true'"
  // the brandNew function should return true if it was built this year (2015) - hardcode this year for now
);
assert(
  bikes[0].brandNew === bikes[5].brandNew, "only one implementation of the brandNew() function should exist"
);

console.log("All tests passed");