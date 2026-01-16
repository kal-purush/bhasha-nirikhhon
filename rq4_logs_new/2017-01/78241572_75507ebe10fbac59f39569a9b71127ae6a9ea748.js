

var points = 5;
var score = $("h2");
var card = $(".game li");


var modal = document.getElementById('myModal');
var btn = document.getElementById("myBtn");
var span = document.getElementsByClassName("close")[0];


btn.onclick = function() {
    modal.style.display = "block";
}
span.onclick = function() {
    modal.style.display = "none";
}
window.onclick = function(event) {
    if (event.target == modal) {
        modal.style.display = "none";
    }
}

card.click(function () {
  
  if ( $(this).hasClass('matched') ) {
    return;
}
  
 
  $(this).toggleClass("flipped");
  
  
  var flipped = $(".flipped").not(".matched");
  
   
  
  if ( flipped.length === 2 ) {
    
    var firstCard = flipped.first();
    var secondCard = flipped.last();

   
    if ( firstCard.text() === secondCard.text() ) {
    
      score.text("You've found a match."); 
      firstCard.addClass("matched");
      secondCard.addClass("matched");
    } 
    else if(firstCard.text() !== secondCard.text() ) {
        points--;
        score.text("Muahahaha. Try again!");
        firstCard.removeClass("flipped");
        secondCard.removeClass("flipped");
    if (points===0){
    
    $("#myBtn").click();
    
    function stopGame(){
        return
    }

}

        
}
    else {
      
      setTimeout(function () {
        firstCard.removeClass("flipped");
        secondCard.removeClass("flipped");
    }, 1000);
      
   }
  } 
  
});
$(function(){


var ul = document.querySelector('ul');
for (var i = ul.children.length; i >= 0; i--) {
    ul.appendChild(ul.children[Math.random() * i | 0]);
}
});
// function resetGame(){
//   window.location.reload();
// }

// $(function(){
//   $("resetBtn").on("click",resetGame);
// });
