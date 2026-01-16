$(function () {
    //Marvel API
    var apiMarvelKey = "97a93e9e494106d892973948f5b253d9";
    var timeStamp = "1";
    var hash = "c32debe50244fc7a722036892da77e19";
    var searchResult = prompt("What comic character do you want to search?");
    var marvelQueryURL = "https://gateway.marvel.com/v1/public/characters?name=" + searchResult + "&ts=" + timeStamp + "&apikey=" + apiMarvelKey + "&hash=" + hash;
    //https://gateway.marvel.com/v1/public/characters?name=cyclops&ts=1&apikey=97a93e9e494106d892973948f5b253d9&hash=c32debe50244fc7a722036892da77e19
    console.log(marvelQueryURL);
    $.ajax({
        url: marvelQueryURL,
        method: "GET",
    }).then(function (data) {
        $(".marvelDump").text(JSON.stringify(data,null,4));
    })
});