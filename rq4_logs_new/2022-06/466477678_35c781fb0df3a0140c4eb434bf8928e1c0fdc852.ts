const W3CWebSocket = require('websocket').w3cwebsocket;

// Create websocket for Ogmios on Cardano Box

export const OgmiosWS = new W3CWebSocket(
  sessionStorage.getItem("sessionType") == "cardanobox" ?
    window.location.protocol == "http:" ? 
      `ws://${sessionStorage.getItem("ogmiosURI")}:${sessionStorage.getItem("ogmiosPort")}`
      :
      `wss://${sessionStorage.getItem("ogmiosURI")}:${sessionStorage.getItem("ogmiosPort")}`
  :
    "ws://nosocket"
);

OgmiosWS.onopen = () => {
  console.log("Ogmios connection open")
};

OgmiosWS.onerror = () => {
  console.log('Ogmios Connection Error');
};

export const wsp = (methodname: any, args: any) => {
  OgmiosWS.send(JSON.stringify({
    type: "jsonwsp/request",
    version: "1.0",
    servicename: "ogmios",
    methodname,
    args
  }));
}