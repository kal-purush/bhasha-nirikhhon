var majorCities = {
  BC: ["Vancouver", "Victoria", "Prince George"],
  AB: ["Edmonton", "Calgary"]
};
var printOut = {
  for (var i=0; i<=majorCities.length; i++){
    console.log(majorCities[i]+' has '+i.length+' major cities');
  };
};
printOut();