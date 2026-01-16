$(document).ready(function(e){
  $.ajax({
    url : "/layout/nav.html",
    success : function(res){
      $('.nav-holder').html(res);
    }
  })
})