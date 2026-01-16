$(document).ready(function () {

    const artistArr = ["The Matrix", "Keanu Reeves", "John Wick", "The Waterboy", "Rambo", "Adam Sandler", "The Jerk", "Steve Martin"];

    function makeButton() {
        for (let i = 0; i < artistArr.length; i++) {
            var button = $("<button>");
            button.attr("data-artist", artistArr[i]);
            button.attr("class", "artist-button");
            button.text(artistArr[i]);
            $("#buttonDiv").append(button);
        }
        // Event listener for all button elements
        $(".artist-button").on("click", function () {
            $("#artist-div").empty();
            console.log("button clicked");
            var artist = $(this).attr("data-artist");
            console.log("The artist is: " + artist);


            // Constructing a URL to search Giphy for the name of the person who said the quote
            var url = "https://api.giphy.com/v1/gifs/search?q="
            var apiKey = "&api_key=FgFxekddCZONy49RPItgyQeFHyUM6NcX&limit=10"
            var queryURL = url + artist + apiKey;


            // Performing our AJAX GET request
            $.ajax({
                url: queryURL,
                method: "GET"
            })
                // After the data comes back from the API
                .then(function (response) {
                    // Storing an array of results in the results variable
                    var results = response.data;

                    console.log("results length: " + results.length);
                    console.log(results);
                    // Looping over every result item
                    for (var i = 0; i < results.length; i++) {
                        var gifDiv = $("<div>");
                        gifDiv.attr("class", "img-div");

                        // Storing the result item's rating
                        var rating = results[i].rating;
                        console.log("rating: " + rating);

                        // Creating a paragraph tag with the result item's rating
                        var p = $("<p>").text("Rating: " + rating);

                        // Creating an image tag
                        var artistImage = $("<img>");

                        // Getting the urls for the still and animated images
                        var stillImage = results[i].images.fixed_height_still.url;
                        var animateImage = results[i].images.fixed_height.url;

                        // Giving the src attribute the url of the still image.
                        artistImage.attr("src", stillImage);

                        // Assigning attributes to be used to control still/animate
                        artistImage.attr("data-still", stillImage);
                        artistImage.attr("data-animate", animateImage);
                        artistImage.attr("data-state", "still");
                        artistImage.attr("class", "gif");


                        // Appending the paragraph and personImage we created to the "gifDiv" div we created

                        gifDiv.append(artistImage);
                        gifDiv.append(p);

                        // Prepending the gifDiv to the "#gifs-appear-here" div in the HTML
                        $("#artist-div").prepend(gifDiv);

                    }
                    // Event listener for images after they have been rendered
                    $(".gif").on("click", function () {
                        // The attr jQuery method allows us to get or set the value of any attribute on our HTML element
                        var state = $(this).attr("data-state");
                        // If the clicked image's state is still, update its src attribute to what its data-animate value is.
                        // Then, set the image's data-state to animate
                        // Else set src to the data-still value
                        console.log("State on click: " + state);
                        if (state === "still") {
                            console.log("State is supposed to be still: " + state);
                            $(this).attr("src", $(this).attr("data-animate"));
                            $(this).attr("data-state", "animate");
                        } else {
                            console.log("State is supposed to be animated: " + state);
                            $(this).attr("src", $(this).attr("data-still"));
                            $(this).attr("data-state", "still");
                        }
                    });

                });
        });

    }

    makeButton();

    // Event handler for user clicking the add-artist button
    $("#add-artist").on("click", function (event) {
        // Preventing the button from trying to submit the form
        event.preventDefault();
        // Storing the artist name
        var inputArtist = $("#artist-input").val().trim();
        //console.log("Input artist: " + inputArtist);
        artistArr.push(inputArtist);
        $("#buttonDiv").empty();

        makeButton();
    });
});