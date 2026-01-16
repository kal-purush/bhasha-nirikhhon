import React, { useState } from "react";

const Input = ({ socket }) => {
  const [message, setMessage] = useState("");
  const sendMessage = (event) => {
    event.preventDefault();
    console.log("message", message);
    if (message) {
      socket.emit("sendMessage", message, () => setMessage(""));
    }
  };

  return (
    <div className="input-wrapper">
      <form
        onSubmit={(event) => {
          event.preventDefault();
          sendMessage(event);
        }}
      >
        <input
          type="text"
          value={message}
          onChange={(event) => setMessage(event.target.value)}
          onKeyPress={(event) =>
            event.key === "Enter" ? sendMessage(event) : null
          }
        ></input>
      </form>
      <button type="submit" onClick={(message) => sendMessage(message)}>
        SEND
      </button>
    </div>
  );
};

export default Input;