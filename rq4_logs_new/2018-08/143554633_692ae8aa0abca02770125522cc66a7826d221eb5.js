$(document).ready(function() {

    var displayButtons = 
    ["Umbrella Corporation", "Milla Jovovich", "Clint Culpepper","Resident Evil"];

    function displayImg() {
        $("#displayed-images").empty();
        var input = $(this).attr("data-name");
        var limit = 5;
        var queryUrl = "https://api.giphy.com/v1/gifs/trending?" + input + "&limit=" + limit + "&api_key=ORwGK7EjuRxPr3DoRfH6vUcRReTM7fzJ";
    
        $.ajax({
            url: queryUrl,
            method: "GET"
        }).done(function(response) {
            console.log(response);

            for(var i = 0; i < limit; i++) {
                var displayDiv = $("<div>");
                displayDiv.addClass("holder");

                var image = $("<img>");
                image.attr("src", response.data[i].images.original_still.url);
                image.attr("data-still", response.data[i].images.original_still.url);                
                image.attr("data-animate", response.data[i].images.original.url);                
                image.attr("data-state", "still");                
                image.attr("class", "gif");
                displayDiv.append(image);

                var rating = response.data[i].rating;
                console.log(response);
                var ratings = $("<p>").text("Rating: " + rating);
                displayDiv.append(ratings);

                $("#display-images").append(displayDiv);
            }
        });
    }

    function btnRender() {
        $("#display-buttons").empty();
        for (var j = 0; j < displayButtons.length; j++) {

            var newBtn = $("<button>")
            newBtn.attr("class", "btn btn-default");
            newBtn.attr("id", "input");
            newBtn.attr("data-name", displayButtons[j]);
            newBtn.text(displayButtons[j]);
            $("#display-buttons").append(newBtn);
        }
    }

    function imageChangeState() {

        var state = $(this).attr("src", animateImage);
        var animateImage = $(this).attr("data-animate");
        var stillImage = $(this).attr("data-still");

        if(state == "still") {
            $(this).attr("src", animateImage);
            $(this).attr("data-state", "animate");
        }

        else if (state == "animate") {
            $(this).attr("src", stillImage);
            $(this).attr("data-state", "still");
        }
    }

    $("#submitPress").on("click", function() {

        var input = $("#user-input").val().trim();
        form.reset();
        displayButtons.push(input);

        return false;
    })

    btnRender();

    $(document).on("click", "#input", displayImg);
    $(document).on("click", ".gif", imageChangeState);
});