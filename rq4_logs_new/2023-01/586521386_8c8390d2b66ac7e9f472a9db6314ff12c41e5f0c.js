var socket = new WebSocket('ws://localhost:9000/ws');

export let connect = (cb) => {
    socket.onopen = () => {
        console.log(" WebSocket connection is established");
    }
        socket.onmessage = (msg) => {
            console.log("Message from WebSocket: ", msg);
           
          }
          socket.onclose = (event) => {
            console.log("socket closed ");
           
          }
          socket.onerror = (error) => {
            console.log("Socket Error: ", error)
          }

      }

      export let sendMsg = (msg) => {
        console.log("sending msg: ", msg);
        socket.send(msg);
      };

