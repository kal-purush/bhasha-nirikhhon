// https://www.freecodecamp.com/challenges/wherefore-art-thou

/*
  Make a function that looks through an array of objects (first argument) and returns an array of all objects that have matching property and value pairs (second argument). Each property and value pair of the source object has to be present in the object from the collection if it is to be included in the returned array.
  The source object is only an object, with one or more properties.
  The collection is an array of several objects.
*/

function whatIsInAName(collection, source) {
  var arr = [];

  var match;
  for (var i = 0; i < collection.length; i++) {
    for (var key in source) {
      if ( collection[i].hasOwnProperty(key) ) {
        console.log(key);
        if ( collection[i][key] == source[key] ) {
          match = true;
          continue;
        } else {
          match = false;
          break;
        }
      } else {
        match = false;
        break;
      }
    }
    if (match) {
      arr.push( collection[i] );
    }
  }
  return arr;
}

whatIsInAName( [{ first: "Romeo", last: "Montague" }, { first: "Mercutio", last: null }, { first: "Tybalt", last: "Capulet" }], { last: "Capulet" } );