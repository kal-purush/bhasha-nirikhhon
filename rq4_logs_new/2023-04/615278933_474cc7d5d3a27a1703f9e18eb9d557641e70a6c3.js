import consumer from "./consumer";
document.addEventListener("turbolinks:load", () => {
  // const senderId = 1; // Replace with the ID of the sender
  // const receiverId = 2; // Replace with the ID of the receiver
  // const roomId = `room-${[senderId, receiverId].sort().join("-")}`;

  consumer.subscriptions.create(
    { channel: "RoomChannel", room_id: 1 },
    {
      connected() {
        // Called when the subscription is ready for use on the server"
        console.log("Connected to the RoomChannel");
      },

      disconnected() {
        // Called when the subscription has been terminated by the server
        console.log("Disconnected from the RoomChannel");
      },

      received(data) {
        // Called when there's incoming data on the websocket for this channel
        console.log(data);
        console.log("message received");
        const messagesContainer = document.getElementById("messages");
        const senderName = data.sender_id;
        const receiverName = data.receiver_id;
        const messageContent = data.message;
        const timestamp = data.timestamp;

        const messageHtml = `
        <div class="message">
          <div class="message-header">
            <span class="sender">${senderName}</span>
            <span class="receiver">${receiverName}</span>
            <span class="timestamp">${timestamp}</span>
          </div>
          <div class="message-content">${messageContent}</div>
        </div>
      `;

        messagesContainer.insertAdjacentHTML("beforeend", messageHtml);
        //  messagesContainer.insertAdjacentHTML(
        //   "beforeend",
        //   "<div>" + msg + "</div>"
        // );
        const inputField = document.getElementById("message_content");
        const senderField = document.getElementById("message_sender_id");
        const receiverField = document.getElementById("message_receiver_id");
        inputField.value = "";
        senderField.value = "";
        receiverField.value = "";
      },

      send(data) {
        const messageInput = document.getElementById("message_content");
        const messageContent = messageInput.value;
        const senderId = 1; // Replace with the ID of the sender
        const receiverId = 2; // Replace with the ID of the receiver

        const messageData = {
          content: messageContent,
          sender_id: senderId,
          receiver_id: receiverId,
          room_id: 1,
        };
        const messageJson = JSON.stringify(messageData);

        App.room.speak(messageJson);

        messageInput.value = "";
      },
    }
  );
});