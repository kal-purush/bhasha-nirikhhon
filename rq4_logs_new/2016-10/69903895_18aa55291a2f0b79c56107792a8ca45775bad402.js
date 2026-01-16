window.alert("20 years ago, a zombie apocolypse plagued the world and Joel lost his daughter at the start of quarantine.. 20 years later, he is living in this plagued world as a mercenary... This is his journey.");

$(document).ready(function(){

  $("#startGame").on("click", function(){
    alert("Let's Get Started!");
    $("#startGame").toggleClass("displayNone");
  });

  $("#startGame").on("click",function(){
    $("#actionBtn").toggle();
    $("#beginStory").append("Joel is awaken by a knock on the door." + "<br />");
  });

  $("#actionBtn").on("click", function(){
    var answersDoor = confirm("Does Joel answer the door?");
    if(answersDoor == true){
      answersDoor = "Joel answers the door.";
    } else {
      answersDoor = "They keep knocking so eventually Joel answers the door";
    }
    $("#beginStory").append(answersDoor + "<br />");
    $("#actionBtn").on("click", function(){
      $("#actionBtn").toggleClass("displayNone");
    });
  });

  $("#actionBtn").on("click", function() {
    $("#keepGoing").toggle();
    $("#beginStory").append("<br />" + "It's Tess and she wants to get revenge on Robert." + "<br />");
  });

  $("#keepGoing").on("click", function() {
    var decideNext = confirm("Does Joel go with her?");
    if(decideNext == true){
      decideNext = "Joel agrees and goes with Tess to end Roberts life.";
    } else {
      alert("Joel decides to stay and the story ends here.");
      location.reload();
    }
    $("#beginStory").append(decideNext + "<br />");
    $("#keepGoing").on("click", function(){
      $("#keepGoing").toggleClass("displayNone");
    });
  });

  $("#keepGoing").on("click", function(){
    $("#keepGoing2").toggle();
    $("#beginStory").append("<br />" + "They kill Robert and run into the leader of the Fireflies, Marlene, who needed Robert for a drop. She bargains with Joel and Tess to do the drop for her." + "<br />");
  });

  $("#keepGoing2").on("click", function() {
    var fireFly = confirm("Does Joel take Marlene's bargain of guns to make the drop?");
    if(fireFly == true){
      fireFly = "Joel and Tess follow Marlene to pick up the drop."
    } else {
      fireFly = "Joel doesn't accept the bargain and Tess kills Marlene, this ends the story.";
      location.reload();
    }
    $("#beginStory").append(fireFly + "<br />");
    $("#keepGoing2").on("click", function(){
      $("#keepGoing2").toggleClass("displayNone");
    });
  });

  $("#keepGoing2").on("click", function() {
    $("#keepGoing3").toggle();
    $("#beginStory").append("<br />" + "They arrive to the pick up location. Joel and Tess find out that they are to deliver a 14 year old girl named Ellie to the Capitol Building." + "<br />");
  });

  $("#keepGoing3").on("click", function() {
    var deliverEllie = confirm("Does Joel agree to take the job with Tess and deliver the girl to the Capitol Building?");
    if(deliverEllie == true) {
      deliverEllie = "Joel and Tess agree and start a journey to deliver Ellie."
    } else {
      deliverEllie = "Joel decides he doesn't want to continue and the game is over";
      location.reload();
    }
    $("#beginStory").append(deliverEllie + "<br />");
    $("#keepGoing3").on("click", function() {
      $("#keepGoing3").toggleClass("displayNone");
    });
  });

  $("#keepGoing3").on("click", function() {
    $("#keepGoing4").toggle();
    $("#beginStory").append("<br />" + "They get to the Capitol Building, but all of the Fireflies you were supposed to deliver Ellie to are dead. Then you find out that Tess has been infected along the way. You hear soliders coming up to the Capitol Building and Tess tells you to run and take Ellie to your brother Tommy." + "<br />");
  });

  $("#keepGoing4").on("click", function(){
    var fightOrStay = confirm("Does Joel listen to Tess?");
    if(fightOrStay == true) {
      fightOrStay = "Joel argues with Tess for a moment and then takes Ellie and leaves.";
    } else {
      fightOrStay = "Joel stays and has a shoot out with the soldiers and everyone dies. GAME OVER.";
      location.reload();
    }
    $("#beginStory").append(fightOrStay + "<br />");
    $("#keepGoing4").on("click", function(){
      $("#keepGoing4").toggleClass("displayNone");
    });
  });

  $("#keepGoing4").on("click", function(){
    $("#theEnd").toggle();
    $("#beginStory").append("<br />" + "Joel and Ellie start to escape. On their way to find a way out, they witness Tess being shot by the soldiers. They finally get out of the Capitol Building and begin their journey to deliver Ellie to the Fireflies." + "<br />");
  });

  $("#theEnd").on("click", function() {
    alert("Thank you for playing this version! Look out for the extended version coming soon!");
  });



});