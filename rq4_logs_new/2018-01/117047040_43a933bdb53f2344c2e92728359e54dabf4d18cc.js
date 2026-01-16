console.log("************************** ");
console.log("****** 20s  30s  40s  50s  ");
console.log("************************** ");
console.log("*Slow* 40   35   30   25 * ");
console.log("*Med** 45   40   35   30 * ");
console.log("*Fast* 50   45   40   35 * ");
console.log("************************** ");


    $("#add-user").on("click", function(event) {
      // prevent form from trying to submit/refresh the page
      event.preventDefault();

      // Capture User Inputs and store them into variables
      var weight = $("#weight-input").val().trim();
      var metabolicRate = $("#metabolicRate-input").val().trim().toLowerCase();
      var age = $("#age-input").val().trim();
      var goal = $("#goal-input").val().trim().toLowerCase();

      // Console log each of the user inputs to confirm we are receiving them
      console.log(weight);
      console.log(metabolicRate);
      console.log(age);
      console.log(goal);

      //other necessary global variables for calculations
      var minCal, normCal, rate, goalCal;

      //calculations
      minCal = weight * 11;
      console.log(minCal);

      //Metabolic Rate Calculations-------------------------------------------------------
      if( metabolicRate == "slow" || metabolicRate == "Slow" || metabolicRate == "SLOW" )
      {
      	if(age == 20)
      	{
      		normCal = (minCal * 0.40) + minCal;
      		console.log("Everyday active calories needed")
      		console.log(normCal);
      	}

      	if(age == 30)
      	{
      		normCal = (minCal * 0.35) + minCal;
      		console.log("Everyday active calories needed")
      		console.log(normCal);
      	}

      	if(age == 40)
      	{
      		normCal = (minCal * 0.30) + minCal;
      		console.log("Everyday active calories needed")
      		console.log(normCal);
      	}

      	if(age == 50)
      	{
      		normCal = (minCal * 0.25) + minCal;
      		console.log("Everyday active calories needed")
      		console.log(normCal);
      	}
      }
      else if( metabolicRate == "medium")
      {
      	if(age == 20)
      	{
      		normCal = (minCal * 0.45) + minCal;
      		console.log("Everyday active calories needed")
      		console.log(normCal);
      	}

      	if(age == 30)
      	{
      		normCal = (minCal * 0.40) + minCal;
      		console.log("Everyday active calories needed")
      		console.log(normCal);
      	}		

      	if(age == 40)
      	{
      		normCal = (minCal * 0.35) + minCal;
      		console.log("Everyday active calories needed")
      		console.log(normCal);
      	}	

      	if(age == 50)
      	{
      		normCal = (minCal * 0.30) + minCal;
      		console.log("Everyday active calories needed")
      		console.log(normCal);
      	}	
      }
      else if(metabolicRate == "fast")
      {
      	if(age == 20)
      	{
      		normCal = (minCal * 0.50) + minCal;
      		console.log("Everyday active calories needed")
      		console.log(normCal);
      	}	

      	if(age == 30)
      	{
      		normCal = (minCal * 0.45) + minCal;
      		console.log("Everyday active calories needed")
      		console.log(normCal);
      	}	

      	if(age == 40)
      	{
      		normCal = (minCal * 0.40) + minCal;
      		console.log("Everyday active calories needed")
      		console.log(normCal);
      	}	

      	if(age == 50)
      	{
      		normCal = (minCal * 0.35) + minCal;
      		console.log("Everyday active calories needed")
      		console.log(normCal);
      	}	
      }
      //--------------------------------------------------------------------------

      //goal calculations
      if(goal == "lose")
      {
      	goalCal = normCal - 500;
      	console.log("If you are trying to lose weight your target goal is: ")
      	console.log(goalCal);
      }
      else if(goal == "maintain")
      {
      	goalCal = normCal;
      	console.log("If you are trying to maintain current weight your target goal is: ")
      	console.log(goalCal);
      }
      else if(goal == "gain")
      {
      	goalCal = normCal + 500;
      	console.log("If you are trying to gain weight your target goal is: ")
      	console.log(goalCal);
      }
      //-----------------------------------------------------------------------------

      //New global variables 
      var fatCal, proteinCal, carbCal, fatG, proteinG, carbG, unit;

      //Macro Calculations

      unit = goalCal / 6;
      fatCal = unit;
      proteinCal = unit * 2;
      carbCal = unit * 3;

      fatG = fatCal / 9;
      proteinG = proteinCal / 4;
      carbG = carbCal / 4;

      // //body fat percent calculations optional
      // var bodyfat, targetfat, currentfat, massneeded, idealweight, lossneeded;

      console.log("**********************************************************");
      console.log("Your target caloric intake is " + goalCal + ".");
      console.log("You can consume " + fatCal + " calories from fat.");
      console.log("You can consume " + fatG + " grams from fat.");
      console.log("**********************************************************");
      console.log("You can consume " + proteinCal + " calories from protein.");
      console.log("You can consume " + proteinG + " grams from protein.");
      console.log("**********************************************************");
      console.log("You can consume " + carbCal + " calories from carbohydrates.");
      console.log("You can consume " + carbG + " grams from carbohydrates.");
      console.log("**********************************************************");

      //Dynamically Add Results to Webpage
      var columnDiv = $("<div>").addClass("grid col-lg-4 col-md-6");
      var cardDiv = $("<div>").addClass("card");
      var cardTitle = $("<p>").text("Your target caloric intake is " + goalCal + ".").addClass("card-title");
      var line1 = $("<p>").text("You can consume " + fatCal + " calories from fat.").addClass("card-title");
      var line2 = $("<p>").text("You can consume " + fatG + " grams from fat.").addClass("card-title");
      var line3 = $("<p>").text("You can consume " + proteinCal + " calories from protein.").addClass("card-title");
      var line4 = $("<p>").text("You can consume " + proteinG + " grams from protein.").addClass("card-title");
      var line5 = $("<p>").text("You can consume " + carbCal + " calories from carbohydrates.").addClass("card-title");
      var line6 = $("<p>").text("You can consume " + carbG + " grams from carbohydrates.").addClass("card-title");

      cardDiv.append(cardTitle).append(line1).append(line2).append(line3).append(line4).append(line5).append(line6);
      columnDiv.append(cardDiv);

      $("#macros-here").append(columnDiv);

      

    });











//-----------------------------------------------------------------------------------
// Google Maps
//-----------------------------------------------------------------------------------
// var map;
// var infowindow;
// var restaurants = [];

// var restaurantName;
// var restaurantAddress;
// var restaurantRating;
// var restaurantPrice;

// var groupName;
// var groupDate;
// var groupParticipants;
// var groupTime;
// var groupTheme;

// $(".overlay").hide();

// function initMap() {
// 	var pyrmont = { lat: 41.896294, lng: -87.618799 };

// 	map = new google.maps.Map(document.getElementById('map'), {
// 			center: pyrmont,
// 			zoom: 15
// 	});

// 	infowindow = new google.maps.InfoWindow();
// 	var service = new google.maps.places.PlacesService(map);
// 	service.nearbySearch({
// 			location: pyrmont,
// 			radius: 800,
// 			type: ['restaurant']
// 	}, callback);
// }

// function callback(results, status) {
// 	if (status === google.maps.places.PlacesServiceStatus.OK) {
// 			for (var i = 0; i < results.length; i++) {
// 					var label0 = i + 1;
// 					var label = label0.toString();
// 					var id = "restaurant" + label;

// 					createMarker(results[i], label);

// 					$("#well").append(
// 							"<h2>" + label + ": " + results[i].name + "</h2>" +
// 							"<p><strong>Rating: </strong>" + results[i].rating + "</p>" +
// 							"<p><strong>Price Level: </strong>" + results[i].price_level + "</p>" +
// 							"<p><strong>Address: </strong>" + results[i].vicinity + "</p>" +
// 							"<button id='" + id + "'>Select Restaurant</button>" +
// 							"<br>");

// 					createClickEvent(id, i);

// 					updateRestaurantsList(results[i]);
// 			}
// 	}
// }

// function createMarker(place, label) {
// 	var placeLoc = place.geometry.location;
// 	var marker = new google.maps.Marker({
// 			map: map,
// 			position: place.geometry.location,
// 			label: label
// 	});

// 	google.maps.event.addListener(marker, 'click', function() {
// 			infowindow.setContent(place.name);
// 			infowindow.open(map, this);
// 	});
// }

// function createClickEvent(id, i) {
// 	$("#" + id).on("click", function() {
// 			$(".overlay").show();

// 			restaurantName = restaurants[i].name;
// 			restaurantAddress = restaurants[i].address;
// 			restaurantRating = restaurants[i].rating;
// 			restaurantPrice = restaurants[i].price;

// 			// console.log(restaurantName, restaurantAddress, restaurantRating, restaurantPrice);

// 	});
// }


// function updateRestaurantsList(results) {
// 	restaurants.push({
// 			"name": results.name,
// 			"rating": results.rating,
// 			"price": results.price_level,
// 			"address": results.vicinity
// 	});
// }


// $("#createGroupBtn").on("click", function(event) {

// 	event.preventDefault();

// 	groupName = $("#groupName").val();
// 	groupDate = $("#groupDate").val();
// 	groupParticipants = $("#groupParticipants").val();
// 	groupTime = $("#groupTime").val();
// 	groupTheme = $("#groupTheme").val();
// });
// //--------------------------------------------------------------------------------------------------
// //--------------------------------------------------------------------------------------------------
