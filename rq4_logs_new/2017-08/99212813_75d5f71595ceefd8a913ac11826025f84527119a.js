var commentList = document.getElementById('commentList');
var commentArea = document.getElementById('commentArea');

var comments = [];

document.getElementById('commentSubmit').onclick = function(){
  var comment = commentArea.value;
  if(comment){
    comments.push(comment);
    // for(var i = 0;i<comments.length;i++){
      var commentListItem = document.createElement("li");
      commentListItem.appendChild(document.createTextNode(comment));
      commentList.appendChild(commentListItem);
      commentArea.value = null;
    // }
  }
}