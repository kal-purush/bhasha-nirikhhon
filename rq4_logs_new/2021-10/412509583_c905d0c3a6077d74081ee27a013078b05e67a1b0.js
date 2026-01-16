$(() => {
    $('form').on('submit', (e) => {
      e.preventDefault()

  	const userInput = $('input[type="text"]').val()


		$.ajax({
				"async": true,
				"crossDomain": true,
				"url": `https://genius.p.rapidapi.com/search?q=${userInput}`,
				"method": "GET",
				"headers": {
					"x-rapidapi-host": "genius.p.rapidapi.com",
					"x-rapidapi-key": "7fa60ae3admsh0d5b06c183ee1cep162fe2jsn4ee4252670d1"
					}

				}).then(function(data) {
					 	console.log(data);
						console.log(data.response);
					const hitsArray = data.response.hits
						for(i = 0; i < hitsArray.length; i++){
							hitsArray[i].result.title
							console.log(hitsArray[i].result.title);
							const $printData = $('<p>').text(hitsArray[i].result.title)
							$('body').append($printData);
						}

						}
				})
		})
})







			// const settings = {
			// "async": true,
			// "crossDomain": true,
			// "url": "https://genius.p.rapidapi.com/search?q=HarryStyles",
			// "method": "GET",
			// "headers": {
			// "x-rapidapi-host": "genius.p.rapidapi.com",
			// "x-rapidapi-key": "7fa60ae3admsh0d5b06c183ee1cep162fe2jsn4ee4252670d1"
			// }
			// };
			//
			// $.ajax(settings).done(function (response) {
			// console.log(response);
			// });