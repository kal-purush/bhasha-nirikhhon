var videoId = "${videoId}";
var player;
var counter = 0;
var maxCounter = 2;

function onYouTubeIframeAPIReady() {
    player = new YT.Player('youtubePlayer', {
        events: {
            'onReady': onPlayerReady,
            'onStateChange': onPlayerStateChange
        }
    });
}

function onPlayerReady(event) {
    // Você pode realizar ações quando o player estiver pronto
}

function onPlayerStateChange(event) {
    // Aqui você pode monitorar mudanças de estado do player
    if (event.data === PlayerState.ENDED) {
        counter++;
        console.log(counter);
        if (counter < maxCounter) {
            player.playVideo();
        }
    }
}