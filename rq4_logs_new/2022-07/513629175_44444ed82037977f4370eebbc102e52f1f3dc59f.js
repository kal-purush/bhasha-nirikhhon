$('document').ready(function() {
    makeRequest();

    function makeRequest() {
        var request = new XMLHttpRequest();

        request.open('GET', "https://api.cricapi.com/v1/currentMatches?apikey=e508836a-74ea-46f1-ad21-145ae3a7c208&offset=0");
        request.onload = function() {
            if(this.status == 200) {
                var response = JSON.parse(this.responseText);

                console.log(response);
                parseData(response.data);
            }
        };
        
        request.send();
    }

    function parseData(data) {
        for (var i = 0; i < 4; ++i) {
            var match = data[i];
            var teams = match["teams"];
            var score = match["score"];

            var matchCard = $('#match' + (i+1));

            $(matchCard).find('#team1').html(teams[0]);
            $(matchCard).find('#team2').html(teams[1]);

            $(matchCard).find('#status').html(match["status"]);

            var score1 = score[0];
            var score2 = score[1];

            if(score1 != null)
                $(matchCard).find('#score1').html(score1["r"] + "/" + score1["w"] + "-" + score1["o"]);
            else
                $(matchCard).find('#score1').html(0 + "/" + 0 + "-" + 0);

            if(score2 != null)
                $(matchCard).find('#score2').html(score2["r"] + "/" + score2["w"] + "-" + score2["o"]);
            else
                $(matchCard).find('#score2').html(0 + "/" + 0 + "-" + 0);

            $(matchCard).find('#venue').html(match["venue"]);
        }
    }
})