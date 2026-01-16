function Quote(){
  $(".quotebody").fadeOut(1);
   $(".authorbody").fadeOut(1);
 $.ajax
 ({

     url: "https://api.forismatic.com/api/1.0/?",
      dataType: "jsonp",
      data: "method=getQuote&format=jsonp&lang=en&jsonp=?",
      success: function(response) 
      {
        
        $(".quotebody").html("<div class=\"quotebody\">" + "<i class=\"fa fa-quote-left\" aria-hidden=\"true\"></i>"+" "+
         response.quoteText +" "+ "<i class=\"fa fa-quote-right\" aria-hidden=\"true\"></i></div>");
        
        if(response.quoteAuthor=="")
          response.quoteAuthor="Unknown Author";
        
        $(".authorbody").html("  <div class=\"authorbody\">" +"- <u>"+ response.quoteAuthor + "</u></div>");
        
        $(".tweet").attr("href", 'https://twitter.com/intent/tweet?text=' + response.quoteText + 'Author - ' + response.quoteAuthor);
        
         
      }
   });  
   $(".quotebody").delay(1000).fadeIn(500);
    $(".authorbody").delay(1000).fadeIn(500);
}

$(document).ready(function(){
   Quote();
  });

  $(".nextquote").click(function(){
  Quote();
  });