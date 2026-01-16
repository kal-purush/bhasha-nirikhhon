$(document).ready(function(){
    $("#currentDay").text(moment().format("MMMM Do YYYY"));

    displayBlocks();
    function displayBlocks(){

        var timeBlocks = $("<div>");
        $(".container").append(timeBlocks);
        $(timeBlocks).text("It Worked");

        for(var i = 0; i<9; i++){
            
        }


    }















})