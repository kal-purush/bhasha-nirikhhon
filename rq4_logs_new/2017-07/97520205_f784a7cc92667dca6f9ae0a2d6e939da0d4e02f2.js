$(document).ready(function(){
  $("select")
    .on("change", function(e){
      var isGoal = this.value == "goal";
      console.log('HELLOOOOOO', isGoal);
      $(".form-deadline").toggleClass("hidden", !isGoal);
    }).change();
})