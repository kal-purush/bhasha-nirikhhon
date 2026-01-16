'use strict';

// This is a very basic sample Service Worker (SW) that  acts as a server for
// navigator.connect. I'm going to mark with a comment where the app MUST
// add some extra code to use the navigator.connect POLYFILL
// So if you just want to know that, search for:
// ADDED FOR POLYFILL

function debug(str) {
  console.log('Setting Service SW -*- -->' + str);
}

// ADDED FOR POLYFILL: Import the polyfill script
this.importScripts('/settings-server/common/polyfill/navigator_connect_sw.js');
// END ADDED FOR POLYFILL

this.addEventListener('install', function(evt) {
  debug('SW Install event');
});

this.addEventListener('activate', function(evt) {
  debug('SW activate event');
});

this.addEventListener('fetch', function(evt) {
  debug('SW fetch event');
});

this.channelToMT = new Promise((resolve, reject) => {
  this.resolveChannel = resolve;
  this.rejectChannel = reject;
});


this._portId = 1;
this._ports = {};

this.channelToMT.then(channel => {
  channel.onmessage = evt => {
    var remotePort = this._ports[evt.data.remotePortId];
    remotePort.postMessage(evt.data.data);
  };
});

// This has a *HUGE* problem currently! It's not safe to accept more than one connection!
// mostly because we're multiplexing the channel to the MT without really multiplexing it
this.onconnect = function(msg) {
  debug('SW onconnect: We should have a port here on msg.source. ' +
        (msg.source.postMessage ? 'yes!' : 'no :('));
  // msg.source should have the endpoint to send and receive messages,
  // so we can do:

  // TO-DO: Not everybody should have access!!!
  msg.acceptConnection(true);

  var remotePort = msg.source;
  var myPortId = this._portId++;
  this._ports[myPortId] = remotePort;

  remotePort.onmessage = aMsg => {

    // TO-DO: We should implement access control here also!
    debug('SW SETTING msg received:' + JSON.stringify(aMsg.data));
    var requestId = aMsg.data.id;
    if (requestId) {
      debug('onmessage. Got Request:' + JSON.stringify(aMsg.data));
      // In sw APIS do not work!!!! We need to request it to the main thread

      // Since this doesn't work the first time, and we don't want to have to
      // do a reload, we'll work around this by making the main thread pass
      // us a MessageChannel to talk to it
      this.channelToMT.then(channel => {
        // TO-DO: Multiplex the channel!
        channel.postMessage({remotePortId: myPortId, remoteData: aMsg.data});
      });
    } else {
      // Hmm...
      debug('onmessage: got a message without id');
    }
  };
};

this.messageListener = evt => {
  debug('SW onmessage ---> '+ JSON.stringify(evt.data));

  // ADDED FOR POLYFILL
  // Since we're using the same channel to process messages comming from the
  // main thread of the app to the SW, and messages coming from the
  // navigator.connect polyfill, we have to distinguish them here. Sadly we
  // can't remove this even if we have MessageChannels because we have to pass
  // the MessageChannels down (connection messages) somehow.
  if (this.NCPolyfill.isInternalMessage(evt)) {
    debug('SW msg is internal, do not process');
    return;
  }
  // END ADDED FOR POLYFILL

  // Your code here
  // The only message we should get here is a MessageChannel to talk back to
  // the main thread... so...
  if (evt.ports && evt.ports[0]) {
    debug('Got a channel from the parent');
    this.resolveChannel(evt.ports[0]);
  } else {
    debug('Did not got a channel!');
    this.rejectChannel('I did not got a channel');
  }
  // And I can remove the listener, I don't need this anymore
  this.removeEventListener('message', this.messageListener);

};

this.addEventListener('message', this.messageListener);
