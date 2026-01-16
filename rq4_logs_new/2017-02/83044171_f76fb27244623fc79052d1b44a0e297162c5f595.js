
var requestURL = "https://koivist9.firebaseio.com/.json"

/* SLIDE TEKSTIN VAIHTAMINEN */
var slideContents = $.getJSON(requestURL, function (data) {

    	var mainJSON = data;	
    	console.log(mainJSON);
    
    	function changeOne() {
    		$(".main-text").html(mainJSON.slide1.content);
    	}
    	changeOne();

    	function changeTwo() {
    		$(".main-text").html(mainJSON.slide2.content);
    	}
    	setTimeout(changeTwo, 4000);

    	function changeThree() {
    		$(".main-text").html(mainJSON.slide3.content);
    	}
    	setTimeout(changeThree, 8000);

    }
);

$(".bg-image").velocity({scale: 1.2}, {duration:10000,easing:'swing',loop:true})

.velocity("reverse");





/*
$(document).ready(function(){
    $("button").click(function(){
        $.getJSON("https://koivist9.firebaseio.com/.json", function(result){
            console.log(result);
        });
    });
});*/
