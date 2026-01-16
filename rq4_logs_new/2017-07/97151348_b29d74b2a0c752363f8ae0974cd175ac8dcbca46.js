function addComments(){
  var comment = $('#comment').val();
  $('<h4>'+ comment+'</h4>').appendTo('#commentsSection');
  }

function friendsButton(){
  window.location.replace("friendsBlog.html");
}
function photosButton(){
  window.location.replace("photosBlog.html");
}
function blogButton(){
  window.location.replace("blog.html");
}
$('#submitComment').click(addComments);
$('#friends').click(friendsButton);
$('#photos').click(photosButton);
$('#blog').click(blogButton);