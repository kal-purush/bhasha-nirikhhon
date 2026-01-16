  $(function() {
    $(".button").click(function() {
      // get rating from index.html 
     var rating= $("input[name='options']:checked").val();
      
     alert(rating)
     $.ajax({
       type: "POST",
       url: "process.php",
       data: {rating : rating },
       success: function() {
        $('#contact_form').html("<div id='message' style='text-align:center'></div>");
      
       $('#message').html("<img style='width:15%' id='checkmark' src='check.png' />")
        .append("<h2 style='font-size:50px;'>Thank you for your feedback</h2>")
        .append("<p style='font-size:40px;'>It means a lot to us</p>")
       }
   
     });
   
      return false; 
   
    });

 });