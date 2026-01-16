import consumer from "./consumer"

const chatChannel = consumer.subscriptions.create("RoomChannel", {
  connected() {
    
    // Called when the subscription is ready for use on the server
  },

  disconnected() {
    // Called when the subscription has been terminated by the server
  },

  received(message) {
    const messages = document.getElementById('messages')
    messages.innerHTML += message
    // Called when there's incoming data on the websocket for this channel
  },

  speak: function(content) {
    return this.perform('speak', {message: content});
  }
});

document.addEventListener('DOMContentLoaded', function(){
  var input = document.getElementById('chat-input')
  var button = document.getElementById('button')
  button.addEventListener('click', function(){
    var content = input.value
    chatChannel.speak(content)
    input.value = ''
  })
})