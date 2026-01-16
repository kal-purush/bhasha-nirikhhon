


// Event listener for all button elements
$("#searchbtn").on("click", function() {
    event.preventDefault();
    // In this case, the "this" keyword refers to the button that was clicked
     var cityname = $("#search-text").val();
     console.log("searchbtn city=" + cityname);

    // Constructing a URL to search Giphy for the name of the person who said the quote
     var queryURL = "http://api.openweathermap.org/data/2.5/weather?q="+ cityname + "&units=imperial&APPID=86689c86634b54250ac08a4458bd5c6c";  //today's forcast
   // var queryURL = "http://api.openweathermap.org/data/2.5/forecast?q=" + cityname + "&units=imperial&APPID=86689c86634b54250ac08a4458bd5c6c"; //5day forecast
    // var queryURL= "http://api.openweathermap.org/data/2.5/forecast?id=524901&APPID=86689c86634b54250ac08a4458bd5c6c" ;                                                             
    // Performing our AJAX GET request
    $.ajax({
      url: queryURL,
      method: "GET"
    })
      // After the data comes back from the API
      .then(function(response) {

        console.log(response);

        /******Today's Date */
        // Get city name
        cityname = response.name;
        console.log("city= " + cityname);
        //Get date
        todaydate = "12/11/19";
        //create jquary for the date
        // var newdiv= $(".row").addClass("row current-weather");
        var todaycity = $("<h3>").text(cityname + " ("+todaydate + ")");
        //add class to today city
        todaycity.addClass("today-city");
        $(".current-weather").append(todaycity);
                //***************get the icon
        // var wicon = response.weather[0].icon;
        // var wiconEl = $("<i>").addClass(wicon);
        // $(".today-city").append(wiconEl);

        //get humidity
        var humidity = response.main.humidity;
        var newDiv =$("<div>").addClass("row current-humid");
         var humdisplay = $("<p>").text("Humidity: " + humidity + "%");
        humdisplay.addClass("today-disp");
        var newHum = newDiv.append(humdisplay);
          $(".row current-humid").appendTo( ".today-city" );
        // $(".current-weather").append(humdisplay);
        //  $(".5day").prepend(humdisplay);

        //get temp
        var temp = response.main.temp;
        console.log("temp=" + temp);
        var tempdisplay= $("<p>").text("Temperature: " + temp + "°F");
         tempdisplay.addClass("today-disp");
         $(".5day").prepend(tempdisplay);

        //get windspeed
        var windspeed = response.wind.speed;
        var windisplay = $("<p>").text("Wind Speed: " + windspeed + " MPH");
         windisplay.addClass("today-disp");
         $(".5day").prepend(windisplay);


        //get Uv index

        
        

       // var humidity = response.main.humidity;

        
       // var windspeed = response.wind.speed;

        // // Looping over every result item
        // for (var i = 0; i < results.length; i++) {

        //   // Only taking action if the photo has an appropriate rating
        //   if (results[i].rating !== "r" && results[i].rating !== "pg-13") {
        //     // Creating a div for the gif
        //     var gifDiv = $("<div>");

        //     // Storing the result item's rating
        //     var rating = results[i].rating;

        //     // Creating a paragraph tag with the result item's rating
        //     var p = $("<p>").text("Rating: " + rating);

        //     // Creating an image tag
        //     var personImage = $("<img>");

        //     // Giving the image tag an src attribute of a proprty pulled off the
        //     // result item
        //     personImage.attr("src", results[i].images.fixed_height.url);

        //     // Appending the paragraph and personImage we created to the "gifDiv" div we created
        //     gifDiv.append(p);
        //     gifDiv.append(personImage);

        //     // Prepending the gifDiv to the "#gifs-appear-here" div in the HTML
        //     $("#gifs-appear-here").prepend(gifDiv);
        //   }
        // }
      });
  });