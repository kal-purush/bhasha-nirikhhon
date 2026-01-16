var apiKEY,
	matchID,
	lastGame;

$('#btnUpdate').on('click', function () {
	console.log("btnUpdate pressed!");
	apiKEY = $('#apiKey').val();
	matchID = $('#matchID').val();
	console.log("Set values apiKEY: "+apiKEY+" | matchID: "+matchID);
	if (apiKEY != "") {
		console.log("Trying to request info from API");
		$.get('https://osu.ppy.sh/api/get_match', { k: apiKEY, mp: matchID })
			.done(function(response){
				console.log("Got response!");
				document.getElementById('resultTable').innerHTML = "<tr><th>Slot</th><th>Score</th><th>Accuracy</th><th>300s</th><th>100s</th><th>50s</th><th>Miss</th><th>Status</th></tr>";
				lastGame = response.games[response.games.length-1];
				document.getElementById('latestMap').innerHTML = lastGame.beatmap_id;
				lastGame.scores.forEach(function(item){
					var passed;
					if (item.pass == "1") {
						passed = "PASSED";
					}else{
						passed = "FAILED";
					}
					var a = parseInt(item.count300);
					var b = parseInt(item.count100);
					var c = parseInt(item.count50);
					var d = parseInt(item.countmiss);
					var actualSlot = parseInt(item.slot)+1;
					var accuracy = (a*300+b*100+c*50)/(3*(a+b+c+d));
					console.log("Calculated accuracy for slot "+ actualSlot +" to be "+accuracy);
					document.getElementById('resultTable').innerHTML += "<tr><td>"+actualSlot+"</td><td>"+item.score+"</td><td>"+accuracy+"</td><td>"+a+"</td><td>"+b+"</td><td>"+c+"</td><td>"+d+"</td><td>"+passed+"</td></tr>";
				});
			});
	}
});