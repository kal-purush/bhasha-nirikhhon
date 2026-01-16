// ****************** Client Logic ****************** //

// Initialize and subscribe to shared collection
Game = new Mongo.Collection('game');
Meteor.subscribe('game', function(){

});

// Initialize state of players
Template.player1.rendered = function () {
	Game.update({_id: "1"}, {$set: {player1: "here"}});
}

Template.player2.rendered = function () {
	Game.update({_id: "1"}, {$set: {player2: "here"}});
}

// ****************** Utility Function ****************** //

// Check to see if both players are connected
Template.game_play.connected = function() {
	var data = Game.find({_id: "1"}).fetch()[0];
	
	if (data.player1 == "here" && data.player2 == "here") return true;
	
	return false;
}

// Check to see if both players chose either a rock/paper/scissors
Template.game_play.choosing = function() {
	var data = Game.find({_id: "1"}).fetch()[0];
	
	if (data.move1 == "none" || data.move2 == "none") {
		return true;
	}
	
	return false;
}

// Get value chosen for the current game by each player
Template.game_play.choice = function() {
	var data = Game.find({_id: "1"}).fetch()[0];
	
	if (Router.current().route.path() == "/player1") {
		return data.move1;
	} 
	
	return data.move2;
}

// See whether the other player has decided
Template.game_play.other_choice = function() {
	var data = Game.find({_id: "1"}).fetch()[0];
	
	if (Router.current().route.path() == "/player1") {
		if (data.move2 == "none") {
			return "none";
		} else {
			return "chosen";
		}
	} 
	
	if (data.move1 == "none") {
		return "none";
	}
	return "chosen";
}

// Get result of player 1
Template.result.player1_result = function() {
	var data = Game.find({_id: "1"}).fetch()[0];
	return data.move1;
}

// Get result of player 2
Template.result.player2_result = function() {
	var data = Game.find({_id: "1"}).fetch()[0];
	return data.move2;
}

// Determine the winner of the game
Template.result.winner = function() {
	var data = Game.find({_id: "1"}).fetch()[0];
	
	if (data.move1 == data.move2) {
		return "It's a tie!"
	}else if (data.move1 == "rock" && data.move2 == "scissors" ||
			  data.move1 == "paper" && data.move2 == "rock" ||
			  data.move1 == "scissors" && data.move2 == "paper") {
		return "Player 1 Wins!";
	}
	
	return "Player 2 Wins!";
}

// ****************** BUTTON EVENTS ******************

Template.game_rock.events = {
  'click': function(){
	if (Router.current().route.path() == "/player1") {
		Game.update({_id: "1"}, {$set: {move1: "rock"}});
	} else {
		Game.update({_id: "1"}, {$set: {move2: "rock"}});
	}
  }
};

Template.game_paper.events = {
  'click': function(){
	if (Router.current().route.path() == "/player1") {
		Game.update({_id: "1"}, {$set: {move1: "paper"}});
	} else {
		Game.update({_id: "1"}, {$set: {move2: "paper"}});
	}
  }
};

Template.game_scissors.events = {
  'click': function(){
	if (Router.current().route.path() == "/player1") {
		Game.update({_id: "1"}, {$set: {move1: "scissors"}});
	} else {
		Game.update({_id: "1"}, {$set: {move2: "scissors"}});
	}
  }
};

Template.game_reset.events = {
  'click': function(){
	  Game.update({_id: "1"}, {$set: {move1: "none", move2: "none"}});
  }
};