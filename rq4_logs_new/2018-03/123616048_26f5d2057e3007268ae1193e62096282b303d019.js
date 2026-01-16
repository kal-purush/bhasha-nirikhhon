function go() {
    $.ajax({
        url: 'https://opentdb.com/api.php?amount=30',
        type: 'GET',
        //crossDomain: true,
        //dataType: 'jsonp',


        success: function (result) {
            console.log(result.results);
            sort(result.results);
        },
        error: function () {
            alert('Failed!');
        }
    });
}


function sort(resultArray) {
    //differentiate between difficulties
    //differentiate between multiple/not
    var hardArray = [];
    var mediArray = [];
    var easyArray = [];

    var multiArray = [];
    var singleArray = [];

    //checks difficulty
    for (var x = 0; x < resultArray.length; x++) {
        if(resultArray[x].difficulty === "hard") {
            hardArray.push(resultArray[x]);
        } else if(resultArray[x].difficulty === "easy") {
            easyArray.push(resultArray[x]);
        } else {
            mediArray.push(resultArray[x]);
        }
    }

    //checks if multiple options {
    for (var y = 0; y < resultArray.length; y++) {
        if(resultArray[y].type === "multiple") {
            multiArray.push(resultArray[y]);
        } else {
            singleArray.push(resultArray[y]);
        }
    }
    //whatever array the player picked
    setUp(resultArray);
}

function setUp(resultArray) {
    var htm = "";
    for(var i = 0; i < resultArray.length; i++) {
        htm += "<tr><td>"+resultArray[i].difficulty+"</td><td>"+resultArray[i].category+"</td>";
        htm += "<td>"+resultArray[i].question+"</td><td class = 'secret'>"+resultArray[i].correct_answer+"</td></tr>";
    }



    $('#hi').html(htm);
}