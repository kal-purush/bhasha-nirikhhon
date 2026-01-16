
// 2. This code loads the IFrame Player API code asynchronously.
var tag = document.createElement('script');
tag.id = 'proof-api';
tag.src = 'https://www.youtube.com/iframe_api';
var firstScriptTag = document.getElementsByTagName('script')[0];
firstScriptTag.parentNode.insertBefore(tag, firstScriptTag);

// 3. This function creates an <iframe> (and YouTube player)
//    after the API code downloads.
var playerID = '';
var player;
var urls = [];
urls = ['www.youtube.com/embed/vvvX5QM4z3Y',
  'www.youtube.com/embed/PniJM5IwXfE',
  'www.youtube.com/embed/eUkSTnUK_T0'];

function onYouTubeIframeAPIReady() {
  for (var i = 0; i < urls.length; i++) {
    playerID = i;
    player = 'player';
    console.log('playerID', playerID);
  $('#videos').append(
       '<iframe id="player" class=' + playerID + '' +
        'width="640" height="480"' +
        'src="https://' + urls[i] + '?enablejsapi=1"' +
        'frameborder="0"' +
        'style="border: solid 4px #37474F"' +
        'allowfullscreen="allowfullscreen"' +
      '></iframe>'
                    );
  player = new YT.Player(player, {
          events: {
            onReady: onPlayerReady,
            onStateChange: onPlayerStateChange,
          },
        });
      };
}

// 4. The API will call this function when the video player is ready.
function onPlayerReady(event) {
  console.log('player is ready:', playerID);
}

// 5. The API calls this function when the player's state changes.
//    The function indicates that when playing a video (state=1),
//    the player should play for six seconds and then stop.

function onPlayerStateChange(event) {
  if (event.data == YT.PlayerState.PLAYING) {
    viewCount[playerID]++;
  };

  console.log('playerID at bottom;', playerID);
  console.log('viewCount[playerID]:', viewCount[playerID]);
  }