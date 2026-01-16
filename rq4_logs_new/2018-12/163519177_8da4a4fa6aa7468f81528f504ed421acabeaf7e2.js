$(function () {
    const socket = io();
    window.setInterval(function(){
      if ($('#m').val()) {
        socket.emit('user typing');
      } else {
        socket.emit('user not typing');
      }
    }, 1000);
    const dots = window.setInterval( function() {
    const dot = document.getElementById("dot");
    if(dot !== null) {
      if (dot.innerHTML.length > 3) 
        dot.innerHTML = "";
      else 
        dot.innerHTML += ".";
    }}, 500);
    
    $('form').submit(function(){
      socket.emit('chat message', $('#m').val());
      $('#m').val('');
      return false;
    });
    socket.on('chat message', function(msg) {
      $('#messages').append($('<li class="list-group-item">').text(msg));
    });
    socket.on('users info', function(info) {
      $('#usersVal').text(info);
    });
    socket.on('disconnected', function() {
      $('#messages').append($('<li class="list-group-item list-group-item-danger" align="center"><i class="fas fa-user-slash"></i> User disconnected</li>'));
    });
    socket.on('connected', function() {
      $('#messages').append($('<li class="list-group-item list-group-item-success" align="center"><i class="fas fa-user"></i> User connected</li>'));
    });
    socket.on('user typing', function() {
      if(!$('#messages').find(".list-group-item-info").length)
        $('#messages').append($('<li class="list-group-item list-group-item-info"><i class="far fa-comment-dots"></i> User is typing<span id="dot">.</span></li>'));          
    });
    socket.on('user not typing', function() {
      if($('#messages').find(".list-group-item-info").length)
        $(".list-group-item-info").remove();                  
    });
  });