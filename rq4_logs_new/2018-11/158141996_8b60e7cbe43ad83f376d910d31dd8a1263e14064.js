if ('serviceWorker' in navigator) {

  navigator.serviceWorker
    .register('./service-worker.js', { scope: './' })
    .then((registration) => {
      console.log("Service Worker Registered",registration);
    })
    .catch((err) => {
      console.log("Service Worker Failed to Register", err);
    })

}


let get = (url) => {
	return new Promise((resolve, reject) =>{
		let xhr = new XMLHttpRequest();
		xhr.onreadystatechange = () => {
			if(xhr.readystate === XMLHttpRequest.DONE){
				if(xhr.status === 200) {
					let result = xhr.responseText
					result = Json.parse(result);
					resolve(result);
				}else{
					reject(xhr);
				}
			}
		};

		xhr.open('GET', url, true);
		xhr.send();
	});
};

get('https://api.nasa.gov/planetary/apod?api_key=iXGUx8zfKkM4Q3cTkr9DP9ePZ3DqDdPGNFssYTaX')
	.then((response) => {
		document.getElementById('targetImage')[0].src = "https://apod.nasa.gov/apod/image/1811/creatureaurora_salomonsen_960.jpg";
	})
	.catch((err) => {
		console.log('Erro', err);
	})