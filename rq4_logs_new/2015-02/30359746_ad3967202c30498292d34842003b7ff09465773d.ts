var iceServers = {
    iceServers: [
        //{'url': 'turn:yegor.tk'}
        {'url': 'stun:stun.l.google.com:19302' }
    ]
};




var mediaConstraints = {
    optional: [],
    mandatory: {
        OfferToReceiveAudio: false,
        OfferToReceiveVideo: false
    }
};

interface RtcChannelHandler {
    opened();
    //closed();
    iAmFirst();
    received(Message);
}


function OnWebSocketMessage(evt) {
    var obj = JSON.parse(evt.data);
    console.debug('Web socket message received: ' + evt.data);
    if (obj.variant in wsHandlers) {
        wsHandlers[obj.variant](obj.fields);
    }
    else {
        console.warn('Unrecognized web socket message: ' + obj.variant);
    }
}

    var handler: RtcChannelHandler;

    var offerer: webkitRTCPeerConnection;
    var ice = [];
    // Let us open a web socket
    var ws: WebSocket;
    var channel : RTCDataChannel;

class RtcChannel {
    constructor(channelHandler: RtcChannelHandler) {
        handler = channelHandler;
        ws = new WebSocket("ws://yegor.tk:2794" + document.location.pathname);
        ws.onmessage = OnWebSocketMessage;
        offerer = new webkitRTCPeerConnection(iceServers);

offerer.onsignalingstatechange = function (event) {
    console.log(offerer.signalingState);
}
offerer.onicecandidate = function (event) {
    if (!event || !event.candidate) return;

    var obj: any = new Object();
    obj.variant = "IceCandidate";
    obj.fields = event.candidate;
    ws.send(JSON.stringify(obj));

};
        offerer.ondatachannel = function (event) {
            channel = event.channel;

            event.channel.onopen = function () {
                handler.opened();
                console.log("channel onopen");
            };

            event.channel.onmessage = function(event) { handler.received(event.data); };

            console.log(event);
        };

    }

    send(data) {
        channel.send(data);
    }
}

function CreateOffer() {
    channel = offerer.createDataChannel('channel', {});
    channel.onmessage = function(event) { handler.received(event.data); };
    channel.onopen = function () {
        handler.opened();
        console.log("channel.open");
    };

    channel.onerror = function (e) {
        console.error('channel.onerror', JSON.stringify(e, null, '\t'));
    };

    channel.onclose = function (e) {
        console.warn('channel.onclose', JSON.stringify(e, null, '\t'));
    };

    offerer.createOffer(function (offer) {
        offerer.setLocalDescription(offer, function () {
            // Web Socket is connected, send data using send()

            var obj: any = new Object();
            obj.variant = "FirstSessionDescription";
            obj.fields = offer;
            ws.send(JSON.stringify(obj));
        });
    }, function(error) {alert(error)}, mediaConstraints);

}

var wsHandlers = {};
wsHandlers['RequestOfOffer'] = function(data) {
    handler.iAmFirst();
    CreateOffer();
}

wsHandlers['FirstSessionDescription'] = function(data) {
    var desc = new RTCSessionDescription(data);
    offerer.setRemoteDescription(desc, function() {
        for (var index = 0; index < ice.length; index++) {
            offerer.addIceCandidate(new RTCIceCandidate(ice[index]));
            //console.log(ice[index]);
        }
        ice = [];
        offerer.createAnswer(function (answer) {
            offerer.setLocalDescription(answer);
            var obj:any = new Object();
            obj.variant = "SecondSessionDescription";
            obj.fields = answer;
            ws.send(JSON.stringify(obj)); },
                             function(error) {alert(error)}, mediaConstraints);
    });
}

wsHandlers['SecondSessionDescription'] = function(data) {
    //var desc = new RTCSessionDescription({sdp: obj.fields, type:"answer"});
    var desc = new RTCSessionDescription(data);
    offerer.setRemoteDescription(desc);
    //console.log("answerer: " + obj.fields.sdp);
    for (var index = 0; index < ice.length; index++) {
        offerer.addIceCandidate(new RTCIceCandidate(ice[index]));
        //console.log(ice[index]);
    }
    ice = [];
}

wsHandlers['IceCandidate'] = function(data) {
    //console.log(obj.fields);
    if (offerer.remoteDescription != null) {
        offerer.addIceCandidate(new RTCIceCandidate(data));
    } else {
        ice[ice.length] = data;
    }
}