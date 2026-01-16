import * as io from "socket.io-client";

let socket = io.connect(null, { transports: [ "websocket" ], reconnect: false });

socket.on("connect", () => {
  alert("connected!");
});