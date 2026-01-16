
  
  Meteor.subscribe('thePlayers');


  Template.leaderboard.helpers({
    'player': function(){
        //var currentUserId = Meteor.userId();
        return PlayersList.find({}, {sort: {score: -1, name: 1}});
    },
    'otherHelperFunction': function(){
        return "Some other function";
    },
    'selectedClass': function(){
    	var playerId = this._id;
	    var selectedPlayer = Session.get('selectedPlayer');
	    if(playerId == selectedPlayer){
	        return "selected";
	    }
    },
    'showSelectedPlayer': function(){
        var selectedPlayer = Session.get('selectedPlayer');
        console.log(selectedPlayer);
        return PlayersList.findOne(selectedPlayer);
      }
	 
  });

  Template.leaderboard.events({
    // events go here
     'click .player': function(){
        // code goes here
       	var playerId = this._id;
    	 Session.set('selectedPlayer', playerId);
      },
      'click .increment': function(){
        // code goes here
        var selectedPlayer = Session.get('selectedPlayer');
        //Meteor.call('increment', selectedPlayer);
        Meteor.call('modifyPlayerScore', selectedPlayer, 5);
      },
      'click .decrement': function(){
        var selectedPlayer = Session.get('selectedPlayer');
       // Meteor.call('decrement', selectedPlayer);
       Meteor.call('modifyPlayerScore', selectedPlayer, -5);
      },
      'click .remove': function(){
        // code goes here
        var selectedPlayer = Session.get('selectedPlayer');
        Meteor.call('removePlayer', selectedPlayer);
      }
      
  });
  
  Template.addPlayerForm.events({
      // events go here
      'submit form': function(event){
        // code goes here
        event.preventDefault();
        var playerNameVar = event.target.playerName.value;

       Meteor.call('insertPlayerData', playerNameVar);
      }
      
  });
