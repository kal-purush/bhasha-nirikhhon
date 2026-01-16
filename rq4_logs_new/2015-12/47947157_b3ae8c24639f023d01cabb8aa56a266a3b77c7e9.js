window.onload = function() {
  var socket = new WebSocket('ws://echo.websocket.org');
  
  // Get references to elements on the page.
  var form = document.getElementById('message-form');
  var messageField = document.getElementById('message');
  var messagesList = document.getElementById('messages');
  var socketStatus = document.getElementById('status');
  var closeBtn = document.getElementById('close');

  // Show a connected message when the WebSocket is opened.
  socket.onopen = function(event) {
    socketStatus.innerHTML = 'Connected to: ' + event.currentTarget.url;
    socketStatus.className = 'open';
  };

  form.onsubmit = function(e) { // Submit message
    e.preventDefault();
    var message = messageField.value;
    socket.send(message);
    messagesList.innerHTML += '<li class="sent"><span>Sent:</span>' + message +
                              '</li>';
    messageField.value = '';
    return false;
  };

  socket.onmessage = function(event) { // Receive messages
    var message = event.data;
    messagesList.innerHTML += '<li class="received"><span>Received:</span>' +
                               message + '</li>';
  };  

  socket.onerror = function(error) { // Handle errors
    console.log('WebSocket Error: ' + error);
  };

  socket.onclose = function(event) { // Handle disconnection
    socketStatus.innerHTML = 'Disconnected from WebSocket.';
    socketStatus.className = 'closed';
  };

  closeBtn.onclick = function(e) { // Close websocket
    e.preventDefault();
    socket.close();
    return false;
  };
};