let localStream;

const peer = new Peer({
    key: "y0ur-api-key-here",
    debug: 3,
});

const ss = ScreenShare.create({
    debug: true
});

var app = new Vue({
    el: '#app',
    data: {
        myId: "",
        peerId: "",
        video: {}
    },
    methods: {
        startScreen: function(stream) {
            ss.start({
                    width: 1280,
                    height: 720,
                    frameRate: 30,
                })
                .then(function(stream) {
                    video.src = window.URL.createObjectURL(stream);
                    localStream = stream;
                })
                .catch(function(error) {});
        },
        startWatch: function() {
            const call = peer.call(this.peerId);
            call.on('stream', stream => {
                video.src = window.URL.createObjectURL(stream);
            });
        },
        start: function() {
            (this.peerId === "") ? this.startScreen(): this.startWatch();
        }
    }
});

peer.on('call', call => {
    call.answer(localStream);
});

peer.on('open', function() {
    app.myId = peer.id;
});