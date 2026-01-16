(function($, window, fightClub) {

  /**
  * An array holding fighters selected by user.
  */
  var fighting = [];

  /**
  * A method responsible for selecting fighters.
  */

  fightClub.select = function(e){
     var selected = $(this);
     var selectedId = selected.attr('data-fighter-id');
     addToList(selected);

     if(selected.isSelected == true){
       selected.css('background-color', '#0097A7');
     }
  }

  /**
  * A function responsible for adding a fighter to the fighting list.
  * If user selects three fighters, the first selected is removed from the list.
  */

  function addToList(selected){
    for(var i = 0; i < fighting.length; i++){
      if(fighting[i].id == selected.attr('data-fighter-id')){
        return;
      }
    }
    fighting.unshift(fightClub.fighters[selected.attr('data-fighter-id')]);
    selected.isSelected = true;

    if(fighting.length == 3){
      var last = fighting.pop();
      $('[data-fighter-id="'+last.id+'"]').css('background-color', '#00BCD4');
      $('.btn-circle').css('background-color', '#FF5252');

    }
  }

  /**
  * A function responsible for starting the battle with fighters selected earlier,
  * and also displaying latest winner, and the best fighter at the moment.
  * A fighter with larger number of experience points wins the battle.
  * For every win, a fighter gets 3 experience points.
  * For every loose, a fighter get 1 experience point.
  * In case of a draw, both fighters get 1 point.
  */

  fightClub.startBattle = function(){
    if(fighting.length != 2){
      return;
    }
    var selected1 = fighting[0];
    var selected2 = fighting[1];
    var exp1 = selected1.experience;
    var exp2 = selected2.experience;
    var latestWinner = $('.lastWin');
    var latestHtml;
    var winner = $('.winner');
    var currentHtml;
    if(exp1 > exp2){
      latestHtml =  '<p>Latest winner: </p>' + selected1.name + ' ' + selected2.lastname;
      currentHtml = '<div class="winner">' + selected1.name + ' ' + selected1.lastname + ' wins!</div>';
      selected1.experience += 3;
      selected2.experience += 1;
    }
    else if(exp1 < exp2){
      latestHtml =  '<p>Latest winner: </p>' + selected2.name + ' ' + selected2.lastname;
      currentHtml = '<div class="winner">' + selected2.name + ' ' + selected2.lastname + ' wins!</div>';
      selected2.experience += 3;
      selected1.experience += 1;
    }
    else{
      currentHtml = '<p>Looks like we have a draw!<p>';
      selected1.experience += 1;
      selected2.experience += 1;
    }
    latestWinner.html(latestHtml);
    winner.html(currentHtml);

    fighting = [];
    fightClub.displayFighters();
  }

}(jQuery, window, (fightClub)));