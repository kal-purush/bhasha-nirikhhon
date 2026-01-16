// Exercise 1
// var springColors = [ "AF7575", "EFD8A1", "BCD693", "AFD7DB", "3D9CA8" ];
// var temperatures = [ 47.5, 63.2, 56.7, 53, 51.2 ];
// var pickles = {
//     type: "cat",
//     name: "Pickles",
//     weight: 7,
//     likes: ["sleeping", "purring", "eating butter"]
// };
// window.onload = init;
//
// function init() {
//     console.log(springColors);
//     console.log(temperatures);
//     console.log(pickles);
// }

// Exercise 2
function Pet(type, name, weight, likes) {
    this.type = type;
    this.name = name;
    this.weight = weight;
    this.likes = likes;
}

window.onload = init;

function init() {
  var pickles = new Pet("cat", "Pickles", 7, ["sleeping", "purring", "eating butter"]);
  // console.log(pickles);
  var picklesJSON = JSON.stringify(pickles);
  console.log(picklesJSON);

  var anotherPickles = JSON.parse(picklesJSON);
  console.log(anotherPickles);

  var tilla = new Pet("dog", "Tilla", 25, ["sleeping","eating","walking"]);
  // console.log(tilla);
  // var tillaJSON = JSON.stringify(tilla);
  // console.log(tillaJSON);

  var petsArray = [ pickles, tilla ];
  var petsArrayJSON = JSON.stringify(petsArray);
  // console.log(petsArrayJSON);

}