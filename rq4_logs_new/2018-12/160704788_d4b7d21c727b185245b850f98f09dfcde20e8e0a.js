const requestAPI = async () => {
    let request = 'http://api.openweathermap.org/data/2.5/weather?q=' + document.getElementById('tfCity').value + '&APPID=0afbd2dc40130488ab73ab64f3f9573a';
    const response = await fetch(request);
    /*try{
        
    } catch(error){
        
    }*/
    const myJson = await response.json(); //extract JSON from the http response
    
    if(myJson.message == 'city not found'){
        document.getElementById('temp').textContent = 'La ville ' + document.getElementById('tfCity').value + ' n\'a pas été trouvée';
        return;
    }
    console.log(myJson);
    console.log(document.getElementById('temp'));

    //let temperatureBlock = document.createElement("h1");
    //temperatureBlock.textContent = document.createElement('Température à ' + document.getElementById('tfCity').value + ' ' + (myJson.main.temp -273.15) + ' °C');
    document.getElementById('temp').textContent = 'Température à ' + document.getElementById('tfCity').value + ' ' + Math.round((myJson.main.temp -273.15) * 10) / 10 + '°C';
    //document.body.appendChild(temperatureBlock);
    // do something with myJson
}