/**
Copyright (c) 2014 Espruino Project

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Author: Patrick Van Oosterwijck (patrick@silicognition.com)
**/

var net = require("net");

(function() {
  if (typeof net === 'undefined' || net.Socket===undefined) {
    console.log("No net.Socket - serial_nodesocket disabled");
    return;
  }

  function init() {
    Espruino.Core.Config.add("SERIAL_TCPIP", {
      section : "Communications",
      name : "Connect over TCP Address",
      description : "When connecting, add a menu item to connect to a given TCP/IP address (eg. `192.168.1.2` or `192.168.1.2:23`). Leave blank to disable.",
      type : "string",
      defaultValue : "",
    });
  }

  var socket;
  var readListener;
  var connectionDisconnectCallback;
  var connectionReadCallback;
  var connected = false;

  var getPorts = function(callback) {
    console.log("Socket getPorts, config:", Espruino.Config.SERIAL_TCPIP);
    if (Espruino.Config.SERIAL_TCPIP.trim() != "")
      callback([Espruino.Config.SERIAL_TCPIP]); //['TCP/IP: ' + Espruino.Config.SERIAL_TCPIP]
    else
      callback();
  };

  var openSerial=function(serialPort, openCallback, receiveCallback, disconnectCallback) {

    var host = Espruino.Config.SERIAL_TCPIP.trim();
    var port = 23;
    if (host.indexOf(":") >= 0) {
      var i = host.indexOf(":");
      port = parseInt(host.substr(i+1).trim());
      host = host.substr(0,i).trim();
      if (host=="") host="localhost";
    }

    connectionReadCallback = receiveCallback;
    connectionDisconnectCallback = disconnectCallback;
    socket = new net.Socket();
    socket.setEncoding('utf8');

    // error callback
    socket.on('error', function(err) {
      if (connected) {
        console.error("SOCKET RECEIVE ERROR:", JSON.stringify(info));
        connectionDisconnectCallback();
      } else {
        console.log("Failed to open socket " + host+":"+port, "Error:", err);
        openCallback(undefined);
      }
      connected = false;
    });

    // receive callback
    socket.on('data', function(d) {
      if (connected && connectionReadCallback !== undefined) {
        connectionReadCallback(str2ab(d));
      }
    });

    socket.connect(port, host, function (result) {
      connected = true;
      openCallback({socket:"nodesocket"});
    });
  };

  var str2ab = function(str) {
    var buf=new ArrayBuffer(str.length);
    var bufView=new Uint8Array(buf);
    for (var i=0; i<str.length; i++) {
      bufView[i]=str.charCodeAt(i);
    }
    return buf;
  };


  var closeSerial = function() {
    if (socket) {
      socket.end();
      socket = null;
      connectionDisconnectCallback();
      connectionDisconnectCallback = undefinedl
    }
  };

  var writeSerial = function(data, callback) {
    socket.write(data, callback);
  };

  Espruino.Core.Serial.devices.push({
    "init" : init,
    "getPorts": getPorts,
    "open": openSerial,
    "write": writeSerial,
    "close": closeSerial,
  });
})();