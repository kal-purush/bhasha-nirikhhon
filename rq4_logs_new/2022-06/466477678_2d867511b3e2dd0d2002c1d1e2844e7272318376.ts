import spaceprinterclient from "spaceprinterclient";
const W3CWebSocket = require('websocket').w3cwebsocket;

export const SpacePrinterAPI = new spaceprinterclient({
    transport: {
        type: window.location.protocol == "http:" ? "http" : "https",
        host: window.location.hostname == "armdev" ? "spaceprinter.local" : window.location.hostname,
        port: window.location.protocol == "http:" ? 4441 : 4442
    },
});

// Create Websocket for space printer
export const SpacePrinterWSS = new W3CWebSocket(
  window.location.protocol == "http:" ? 
    'ws://spaceprinter.local:3331/, echo-protocol' : 
    `wss://${window.location.hostname}:3332/, echo-protocol`
);

SpacePrinterWSS.onopen = () => {
  console.log("Space Printer connection open")
};

SpacePrinterWSS.onerror = () => {
  console.log('Space Printer Connection Error');
};

export const blockfrostApi = async ( uri: string, method: string, content: any ) => {
  console.log(JSON.stringify(content));
  let settings:any = {};
  const blockfrostApi: any = localStorage.getItem("blockfrostApi"); 

  let tx = Buffer.from(content, 'hex');

  method == "POST" ?
    settings = {
      method: method,
      body: tx,
      headers: {
        "project_id": blockfrostApi,
        "Content-Type": "application/cbor",
      }
    }
    :
    settings = {
      method: method,
      headers: {
        "project_id": blockfrostApi,
      }
    };

  try {
    const fetchResponse: any = await fetch(uri, settings);
    const data: any = await fetchResponse.json();
    // console.log(data);
    return(data);
  } catch (e) {
    console.log(e);
    return e;
  }; 
};
