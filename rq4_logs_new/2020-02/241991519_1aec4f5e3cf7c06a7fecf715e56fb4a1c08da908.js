function testLog(){
    if('geolocation' in navigator){
        navigator.geolocation.getCurrentPosition(position => {
            document.getElementById('target1').innerHTML = `position: ${position.coords.latitude}, ${position.coords.longitude}`;
        });
        navigator.getBattery().then(batt => {
            document.getElementById('target2').innerHTML = `Battery info: ${batt.level}`
        });
        navigator.vibrate([100,30,100,30,100,30,200,30,200,30,200,30,100,30,100,30,100])
    }else{
        document.getElementById('target1').innerHTML = 'No geolocation available';
    }
}