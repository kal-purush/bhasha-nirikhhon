$(document).ready(function(){
  let url=$(this).attr("href");
  $.ajax({
      url: "http://localhost/dashboard/db/"+url,
      success: function(result){
        $(".reemplazo").html(result);
      }
  });

});