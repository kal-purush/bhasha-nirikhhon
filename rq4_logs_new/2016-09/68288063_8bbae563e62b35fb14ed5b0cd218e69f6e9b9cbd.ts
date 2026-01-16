$(document).ready(function() {
    $('#plugin1').dblclick(function() {
        $(this).fadeOut('fast');
    });
    
    $('#plugin2').hover(function() {
        $(this).fadeOut('fast');
    });

    $('.pull-me').click(function () {
        
       $('.panel').slideToggle('slow'); 
        
        
    });
    
});   

/*$("button").click(function(){
    $.getJSON("http://api.fixer.io/latest", function(result){
        console.log(result);
        $.each(result.rates, function(i, field){
            console.log(i);
            $("div#output").append(i + " " + field + " ");
        });
    });
});*/
var buttonOutputArea = $("div#output");
var allRates = $('#all');

$('#b1').click(function() : void{

    $.ajax({
        type: "GET",
        url: "http://api.fixer.io/latest",

         success: function (data) {
            //console.log("success", data);
            $.each(data.rates, function (i, item) {

                allRates.append(i + " " + item + " ");
                
                if(i === "NZD"){
                    buttonOutputArea.append(i + " " + item);
                }
            });
        }

    });

$("#search").on("click", function() : void{
  var searchTerm = $("#searchTerm").val();
  var url = "https://en.wikipedia.org/w/api.php?action=opensearch&search="+ searchTerm +"&format=json&callback=?";
  $.ajax({
    url: url,
    type: "GET",
    async: false,
    dataType: "json",
    success: function(data, status, jqXHR){
      // console.log(data);
      for(var i = 0; i < data[1].length; i++) {
        $("#wikiOutput").prepend("<div><div class='well'><a href="+data[3][i]+"><h2>" + data[1][i]+ "</h2>" + "<p>" + data[2][i] + "</p></a></div></div>");
        
      }
    }
    
  });
  
});    



});







/*class website {

    url:string;

    facebookLikes:number;

    constructor(url:string, facebookLikes:number){

        this.url = url;
        this.facebookLikes = facebookLikes;
    }
}

var google = new website("google.com", 9);*/



/*class Greeter {
    greeting: string;
    constructor(message: string) {
        this.greeting = message;
    }
    greet() {
        return "Hello, " + this.greeting;
    }
}

var greeter = new Greeter("world");*/