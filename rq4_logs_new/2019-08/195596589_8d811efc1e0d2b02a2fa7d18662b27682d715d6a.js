function sendToSpotify(){
    const link = "https://accounts.spotify.com/authorize";
    //parameters
    const client_id = "d16380c173b1473fac1d716db2e64a66";
    const response_type = "token";
    const redirect_uri = "http://127.0.0.1:5000/SampleHTML.html";
    const scopes = ['playlist-read-collaborative','playlist-modify-private','playlist-modify-private','playlist-modify-public',
    'playlist-read-private'];
    //Link should look like https://accounts.spotify.com/authorize?client_id=5fe01282e94241328a84e7c5cc169164&redirect_uri=http:%2F%2Fexample.com%2Fcallback&scope=user-read-private%20user-read-email&response_type=token&state=123
    var constructedLink = `${link}?client_id=${client_id}&redirect_uri=${redirect_uri}&scope=${scopes.join("%20")}l&response_type=response_type&&show_dialog=true`;
    window.location = constructedLink;
}

function sendData(){

}