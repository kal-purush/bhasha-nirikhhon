// YOUR CODE HERE:
var app = {

  init: function(){
    this.server = 'http://localhost:3000/classes/messages';
    this.roomname = "lobby";
    this.rooms = [];
    $('#username-input').val(window.location.search.substring(1).split('=')[1]);
  },

  send: function(message){
    $.ajax({
      url: this.server,
      type: 'POST',
      data: JSON.stringify(message),
      contentType: 'application/json',
      success: function(data){
        console.log('success');
      },
      error: function(data){
        console.log('error on send');
      }
    });
  },

  fetch: function(self){
    $.ajax({
      url: this.server,
      type: 'GET',
      data: {
        order: '-createdAt'
      },
      success: function(data){
        var results = data;
        app.clearMessages();
        for (var i = 0; i < results.length; i++) {
          if (results[i].roomname === self.roomname){
            app.addMessage(results[i]);
          }
        }
      }
    });
  },

  addMessage: function(data){
    var newChat = $('<div>').addClass('chat');1
    var user = $('<span>').addClass('username').text(data.User.username);
    var text = $('<span>').addClass('text').text(data.text);
    newChat.append(user);
    newChat.append(text);
    $('#chats').prepend(newChat);
  },

  updateChat: function(){
    var self = this;
    app.fetch(self);
    setInterval(function(){
      app.fetch(self);
    }, 1000); // andre 3000
  },

  clearMessages: function(){
    $('#chats').html('');
  },

  addRoom: function(roomName){
    var $room = $('<option>').text(roomName);
    $('#roomSelect').append($room);
    this.roomname = roomName;

  },

  handleSubmit: function(e){
    e.preventDefault();
    var message = {
      username: $('#username-input').val(),
      message: $('#message').val(),
      roomname: app.roomname
    };
    $('#message').val('');
    app.send(message);
  },

  addRoomForm: function(e){
    e.preventDefault();
    app.addRoom($('#newRoom').val());
    $('#newRoom').val('');
  }
};


$(function(){
  app.init();
  app.updateChat();
  // lebronSpam();

  $('#form').submit(app.handleSubmit);

  $('#roomSelect').change(function(){
    app.roomname = $('#roomSelect option:selected').val();
    console.log('room changed to ', app.roomname);
    lebronSpam();
    console.log('lebron james has entered the room');
  });

  $('#roomForm').submit(app.addRoomForm);

});

// LEBRON
var lebron = [
  "...BROWSERFY",
  "...PHP",
  "...Angular",
  "...Level DB",
  "NPM!",
  "...for more info: <a href='http://lebron.technology'>http://lebron.technology</a>",
  "LEBRON JAMES, with NO respect for humanity!!!!!1111!!11!1"
];

var lebronSpam = function(){
  // var choose = function(arr){
  //   return arr[Math.floor(Math.random() * arr.length)];
  // };

  // for (var i = 0; i < 100; i++) {
  //   var message = {
  //     username: "LEBRON STACK",
  //     message: choose(lebron),
  //     roomname: app.roomname
  //   };
  //   app.send(message);
  // }
};
