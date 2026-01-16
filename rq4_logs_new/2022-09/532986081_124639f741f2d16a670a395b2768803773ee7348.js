import Peer from "peerjs";

var lastPeerId;
var peer = new Peer();


peer.on('open', function (id) {
    // Workaround for peer.reconnect deleting previous id
    if (peer.id === null) {
        console.log('Received null id from peer open');
    } else {
        lastPeerId = peer.id;
    }

    console.log('ID: ' + peer.id);
});

export function initializePeers() {

}