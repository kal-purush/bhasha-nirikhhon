
var GameClient = function(game_id) {
  this.url = "";
  this.chat = null;
  this.player = new Player(this);

  this.load_delay = 15000;
  this.next_theme = null;
  this.current_theme = null;
  this.game_url = $(location).attr('href') + "/games/" + game_id;

  alert(game_url);

}

var game = null;

$(document).ready(function(){
  $('#game-wrapper').on("initialize", function(game_id) {
    game = new GameClient(game_id);
    game.initialize();
  });
});

GameClient.prototype.next = function() {

  this.player.stop();

  this.current_theme = this.next_theme;

  this.player.play();

}

GameClient.prototype.handle_event = function(event) {

  if(event.match(/next/)) {
    this.next();
  }
}

GameClient.prototype.test = function() {


  this.player.load(this.get_next_record());


}


GameClient.prototype.initialize = function() {

  this.load_iframe_api();
  this.chat = chat;

}

GameClient.prototype.on_video_cued = function() {

  this.chat.irc_msg("!video_ready");

}


  var theme1 = {
    "videoId": "ygNuRpwZqRU",
    "startSeconds": 0,
    "suggestedQuality": "small"
  }

 var theme2 = {
    "videoId": "tIcR7Xkysds",
    "startSeconds": 0,
    "suggestedQuality": "small"
  }

  var theme3 = {
    "videoId": "Ii1tc493bZM",
    "startSeconds": 0,
    "suggestedQuality": "small"
  }

var records = [ theme1, theme2, theme3 ];

GameClient.prototype.get_next_record = function () {
  return records.pop();
}

GameClient.prototype.load_iframe_api = function() {
  var tag = document.createElement('script');

  tag.src = "https://www.youtube.com/iframe_api";
  var firstScriptTag = document.getElementsByTagName('script')[0];
  firstScriptTag.parentNode.insertBefore(tag, firstScriptTag);
}


function onYouTubeIframeAPIReady() {
  game.init_players();
}

GameClient.prototype.init_players = function(parray) {

  var players = {};

  players["a"] = new YT.Player('player', {
                        height: '0',
                        width: '0',
                        events: {
                          'onReady': game.player.onPlayerReady,
                          'onStateChange': game.player.onStateChange
                        }
                      });

  players["b"] = new YT.Player('playerb', {
                        height: '0',
                        width: '0',
                        events: {
                          'onReady': game.player.onPlayerReady,
                          'onStateChange': game.player.onStateChange
                        }
                      });

  this.player.set(players);
}

GameClient.prototype.on_video_play = function () {

  var record = this.get_next_record();

  if(record == null || record == "undefined") {
    return;
  }

  this.next_theme = record;

  this.player.load(record);

}



var Player = function(game) {

  this.current_player = null;
  this.players = {};
  this.ready = null;
  this.queque = [];
  this.last_load = "b";
  this.game = game;

}

var played = 0;

Player.prototype.play = function() {

  var frameID = this.last_load;
  var p = this.players[frameID];

  this.current_player = p;

  this.current_player.playVideo();

  this.game.on_video_play();

}



Player.prototype.stop = function() {

  if(this.current_player != null) {
    this.current_player.pauseVideo();
  }

}

Player.prototype.set = function(players) {
  this.players = players;
}

Player.prototype.onPlayerReady = function(event) {

  if(this.ready) {

    game.test();

  } else {
    this.ready = "a";
  }

}



Player.prototype.load = function(theme) {


  if(this.last_load == "a" ) {

    this.players["b"].cueVideoById(theme);
    this.last_load = "b";
  } else if(this.last_load == "b") {
    this.players["a"].cueVideoById(theme);
    this.last_load = "a";
  }

}

Player.prototype.onStateChange = function(event) {

  
  var state = event.data;

  switch(state) {
    case YT.PlayerState.CUED:
      game.on_video_cued();
      break;
  }
 

}











