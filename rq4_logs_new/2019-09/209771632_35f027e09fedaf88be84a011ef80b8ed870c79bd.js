module.exports = function CricketScoreKeeper() {

    var wickets = 0;
    var total = 0;
    var error = "Game Over"

    function checkWickets(wicket) {

        if (wicket <= 10) {
            wickets.push(wicket);
            return true;
        }
        else if (wicket >= 10) {
            return error;
        }
    }

    function extractOvers(over) {
        overs = 10;
        over = "-1-3-w"
        for (var i = 0; i < overs.length; i++) {
            var score = overs[i];

            if (score === over){

                return total;
            }
            else {
                return wickets;
            }

        }


    }

    return {
        checkWickets,
        extractOvers

    }

    //    var score = "";
    //     for (var i = 0; i < wickets.length; i++) {
    //         if (wickets <= 10) {
    //             score.push(wickets[i]);
    //         }


    //     }

}