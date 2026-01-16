var city = [ 'nyc', 'sf', 'la','austin','sydney'];

$( "#city-type" ).change(function() {
var currentCity = $( "#city-type").val();
if (currentCity === "Austin") {
  document.querySelector('body').style.backgroundImage = 'url(images/austin.jpg)';
}

else if (currentCity === "Los Angeles") {
  document.querySelector('body').style.backgroundImage = 'url(images/la.jpg)';
}

else if (currentCity === "New York City") {
  document.querySelector('body').style.backgroundImage = 'url(images/nyc.jpg)';
}

else if (currentCity === "Sydney") {
  document.querySelector('body').style.backgroundImage = 'url(images/sydney.jpg)';
}

else if (currentCity === "San Fransisco") {
  document.querySelector('body').style.backgroundImage = 'url(images/sf.jpg)';
}




});