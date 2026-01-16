$( document ).ready(function() {

	var data = d3.csv("https://www.dropbox.com/s/fndicprqe3gmjkh/all_rows.csv?dl=0", function(error, data) {
		if (error) throw error;
		console.log(data); // [{"Hello": "world"}, …]
	});
	

	var previous_song = "";


	var doSearch = function(word, callback) {
		console.log('search for ' + word);
		var url = 'https://api.spotify.com/v1/search?type=track&limit=50&q=' + encodeURIComponent('track:"'+word+'"');
		$.ajax(url, {
			dataType: 'json',
			success: function(r) {
				console.log('got track', r);
				callback({
					word: word,
					tracks: r.tracks.items
						.map(function(item) {
							var ret = {
								name: item.name,
								artist: 'Unknown',
								artist_uri: '',
								album: item.album.name,
								album_uri: item.album.uri,
								cover_url: '',
								uri: item.uri,
								preview: preview_url
							}
							if (item.artists.length > 0) {
								ret.artist = item.artists[0].name;
								ret.artist_uri = item.artists[0].uri;
							}
							if (item.album.images.length > 0) {
								ret.cover_url = item.album.images[item.album.images.length - 1].url;
							}
							return ret;
						})
				});
			},
			error: function(r) {
				callback({
					word: word,
					tracks: []
				});
			}
		});
	}

	var parseFn = function(){	
		for(var elem in data){
			
			if(elem["Title"] != previous_song){
				document.getElementById('title').innerHTML = elem["Title"];
				// load the song from spotify and play for 7 seconds
				doSearch(elem["Title"], function(result) {
						console.log('got word result', result);
						document.getElementById('audiotag1').src = result.preview;
						document.getElementById('audiotag1').play();
				});
				
			}
			
			// wait 10 seconds
			setTimeout(parseFn, 10000);		

			previous_song = elem["Title"];


			}	
	}


	parseFn();


});