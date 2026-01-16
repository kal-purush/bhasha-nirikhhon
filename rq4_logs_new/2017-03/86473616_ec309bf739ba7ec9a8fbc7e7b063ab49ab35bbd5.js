var games = ["Dark Souls", "Bioshock", "Smash bros.", "Mario Party", "Luigi's mansion", "Mario Kart", "Kingdom Hearts", "Final Fantasy", "Call of duty", "Overwatch", "Counter-Strike"];

function displaygameInfo() {
    // debugger;
    var Num = $("#videogameNum").val();
    var game = $(this).attr("data-name");
    var queryURL = "http://api.giphy.com/v1/gifs/search?q=" + game + "&api_key=dc6zaTOxFJmzC&limit=" + Num;

    $.ajax({
            url: queryURL,
            method: "GET"
        })
        .done(function(response) {
            console.log("testing");
            console.log(response);
            var results = response.data;
            var wholediv = $("<div id='wholediv'>")
            for (var i = 0; i < results.length; i++) {
                var gifDiv = $("<div class='gifgame'>");
                var rating = results[i].rating;
                var p = $("<p>").text("Rating: " + rating);
                var gameImage = $("<img>")
                gameImage.attr("src", results[i].images.fixed_height_still.url);
                gameImage.attr('data-still', results[i].images.fixed_height_still.url);
                gameImage.attr('data-animate', results[i].images.fixed_height.url);
                gameImage.attr('data-state', "still");
                gameImage.addClass('gameImage');
                gifDiv.append(p);
                gifDiv.append(gameImage);
                wholediv.prepend(gifDiv);
            }
            $("#games-view").html(wholediv);

        });

}

function renderButtons() {

    $("#buttons-view").empty();

    for (var i = 0; i < games.length; i++) {

        var a = $("<button>");
        a.addClass("game");
        a.attr("data-name", games[i]);
        a.text(games[i]);
        $("#buttons-view").append(a);
    }
}

$("#add-game").on("click", function(event) {
    event.preventDefault();
    var game = $("#game-input").val().trim();

    games.push(game);

    renderButtons();
});

function animate() {
    var state = $(this).attr("data-state");
    if (state === "still") {
        $(this).attr("src", $(this).attr("data-animate"));
        $(this).attr("data-state", "animate");
    } else {
        $(this).attr("src", $(this).attr("data-still"));
        $(this).attr("data-state", "still");
    }
}
$(document).on("click", ".game", displaygameInfo);
$(document).on("click", ".gameImage", animate)
renderButtons();