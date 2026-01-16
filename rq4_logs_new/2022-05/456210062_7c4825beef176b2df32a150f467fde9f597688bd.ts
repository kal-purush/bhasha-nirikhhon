import { io } from "socket.io-client";

const URL = process.env.REACT_APP_STAGE === "prod" ? "52.20.228.225:8787" : "http://localhost:8787";
const socket = io(URL, { autoConnect: false, transports: ["websocket"] });

socket.onAny((event, ...args) => {
  console.log(event, args);
});

export default socket;