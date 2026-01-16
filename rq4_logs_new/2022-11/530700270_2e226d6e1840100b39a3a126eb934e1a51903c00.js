let weather;
let weatherID = 0; // returned in the JSON weather element
let state = 0;
let x = 0;
let windspeed = 0;
let temperature = 0;
let humidity = 0 ;
let i1, i2;
let f1;

function setup() {
  createCanvas(400, 400);
  i1 = loadImage ("assets/kyiv.png");
  i2 = loadImage ("assets/cloud.png");

  f1 = loadFont ("assets/BLUECHERIES.ttf");
  // HERE is the call to get the weather. We build the string first.

  let myCityString =
    "https://api.openweathermap.org/data/2.5/weather?q=Kyiv,Ukraine&units=imperial&";

  //You can also use "zipcode"
  // substitute zip=61820 for q=Normal,IL,US
 

 // let myIDString = "appid=xxxxx"; // put your ID instead of xxxxx
  
  let myIDString = "appid=4d30ee31715bc4479deff531cb81ef19" ;

  let myTotalString = myCityString + myIDString;
  

  loadJSON(myTotalString, gotData); // that gotData function happens when JSON comes back.
}

function gotData(data) {
  weather = data;
  print(weather); // for debugging purposes, print out the JSON data when we get it.
  windspeed = weather.wind.speed;
  temperature = weather.main.temp;
  humidity = weather.main.humidity;

}

function draw() {
  
  
  switch (state) {
      
    case 0:
      if (weather) {
        state = 1;
      }
      break;

    case 1:
      let bg = 0;
      if (temperature < 50) {
        bg = map (temperature, 0, 50, 0, 255);
        background (0, 0, bg) ; 
      } else {
        bg = map(temperature, 50, 100, 0, 255);
        background (bg, 100, 100);  
      }
      image(i1, 65, 115, 275, 200);
      fill("black");
      textFont (f1);
      textSize (24);
      text("What is the weather in " + weather.name + "?", 20, 20);
      text("windspeed is " + windspeed, 20, 40);
      text ("temperature is " + temperature, 20, 60);
      text("humidity is " + humidity, 20, 80);
 

      // cloud
      fill("white");
      noStroke();
      image(i2, x, 5, 250, 175);

      // move the cloud's x position
      x = x + windspeed / 3;
      if (x > width) x = 0-230;

      break;
  }
}