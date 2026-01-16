/// <reference path="typings/tsd.d.ts" />
/// <reference path="./models/peer_manager.ts" />

window.onload = function(){
    var peer = new RobotServer.PeerManager('robot', {
        config: {"iceServers":[
            {"url": "stun:stun.skyway.io:3478"}
        ],optional: [{RtpDataChannels: true}]},
        port: 443,
        secure: true,
        debug: 3} );
    peer.addNeighbor(new RobotServer.Neighbor("oculus-video", RobotServer.NeighborTypeEnum.video));
    peer.addNeighbor(new RobotServer.Neighbor("oculus-data", RobotServer.NeighborTypeEnum.data));

    peer.addListener(peer.OnDataReceivedEvent, (data)=>{
        var json = JSON.parse(data);
        json.type = "oculus";
    });

};
