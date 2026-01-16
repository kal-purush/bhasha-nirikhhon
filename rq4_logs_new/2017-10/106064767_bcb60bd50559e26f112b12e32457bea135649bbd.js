      // Initial array of gifs
      var gifs = ["Mario", "Final Fantasy", "Mario Bros.", "Metal Gear Solid"];
      
      // Function for dumping the JSON content for each button into the div
      function displayGifInfo() {

            $("#gifHolder").empty();
            
            var name = $(this).attr('data-name');
            var queryURL = "http://api.giphy.com/v1/gifs/search?api_key=GL9o5wmiFoFy3G2rvRuOC6I4Ux8rNBCL&q=" + name + "&limit=8";
            
            console.log(name);
                        $.ajax({
                  url: queryURL,
                  method: "GET"
                }).done(function(response) {
          var results = response.data;

// for statement to run through images and create their properties
          for (var i = 0; i < results.length; i++) {
            var gifDiv = $("<div class='item col-3'>");

            var rating = results[i].rating;

            var p = $("<p>").text("Rating: " + rating);

            var personImage = $("<img>");
            personImage.attr("src", results[i].images.original_still.url);
              
            /*  // testing for toggling active and idle status
             $(personImage).on("click", function(){
                    for (var i = 0; i < results.length; i++) {
                    if (personImage.attr.src != results[i].images.original.url) {
                    personImage.attr("src", results[i].images.original.url);
                    } j
                    if (personImage.attr.src == results[i].images.original.url)   {
                    personImage.attr("src", results[i].images.original_still.url);
                    }
                }});
                */
// assign attributes to each version of gif we want to go through
            personImage.attr("still", results[i].images.original_still.url)
            personImage.attr("active", results[i].images.original.url)
            personImage.attr("src", personImage.attr("still"));


           

          //  $("#gifHolder").append(gifDiv).show(slow);

// on click cycle through attributes
            personImage.on("click", function(){

              if($(this).attr("src") === $(this).attr('still')){
                  console.log("still");
                  console.log(this);
                        $(this).attr('src', $(this).attr('active'));

                    } else {
                        $(this).attr('src', $(this).attr('still'));
                        console.log(this);
                    }
          });


// testing limiting the Rating to below R
           /* console.log(rating);
            if (rating === "r") {
            // $(this).hide();
            console.log(this);
            }
           */

// assign each gif to the html and animate it
            gifDiv.prepend(p).hide().show(500);
            gifDiv.prepend(personImage).hide().show(500);



            $("#gifHolder").prepend(gifDiv).hide().show(500);
            console.log(response);
          }
        });
            
    


      }



      // Function for displaying gif buttons
      function renderButtons() {

        // Deleting the buttons prior to adding new gifs
        $("#buttons-view").empty();

        // Looping through the array of gifs
        for (var i = 0; i < gifs.length; i++) {

          // Then dynamicaly generating buttons for each gifs in the array
          // create the button. (<button></button>)
          var a = $("<button>");
          // Adding a class of gifs to our button
          a.addClass("gif");
          // Adding a data-attribute
          a.attr("data-name", gifs[i]);
          // Providing the initial button text
          a.text(gifs[i]);
          // Adding the button to the buttons-view div
          $("#buttons-view").append(a);
        }
      }

      // This function handles events where one button is clicked
      $("#add-gif").on("click", function(event) {
        event.preventDefault();

        // This line grabs the input from the textbox
        var gif = $("#gif-input").val().trim();

        // The gifs from the textbox is then added to our array
        gifs.push(gif);

        // Calling renderButtons which handles the processing of our gifs array
        renderButtons();

      });

      // Generic function for displaying the movieInfo
      $(document).on("click", ".gif", displayGifInfo);

      // Calling the renderButtons function to display the intial buttons
      renderButtons();