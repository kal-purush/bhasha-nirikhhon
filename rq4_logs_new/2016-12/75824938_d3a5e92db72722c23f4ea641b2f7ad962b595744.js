var config = require('./resource/youtube/config.json');

module.exports = function(message, suffix) {
	return new Promise((resolve, reject) => {
		var YouTube = require('youtube-node');
		var youTube = new YouTube();

		youTube.setKey(config.key);

		youTube.search(suffix, 1, function(error, result) {
			if (error) {
				reject(error);
			}
			else if(result.items.length <= 0){
				message.channel.sendMessage("No video found :(").then(() => {
					return resolve('no video found');
				});
			}
			else {
				var watch_link = config.youtube_watch_prefix + result.items[0].id.videoId;
				message.channel.sendMessage(watch_link).then(() => {
					return resolve('ok');
				});
			}
		});
	});
}