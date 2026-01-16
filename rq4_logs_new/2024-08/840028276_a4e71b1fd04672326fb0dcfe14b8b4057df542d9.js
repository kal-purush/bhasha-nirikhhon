
var redirect="http://127.0.0.1:5501/Pages/logged.html";
var client_id="7a70861c037544ef83eefb3496d2a600";
var client_secret="872d498250dc48ffb00aa2744125f724";

let playlistName="S-TierMix";

const AUTHORIZE="https://accounts.spotify.com/authorize";
const TOKEN="https://accounts.spotify.com/api/token";
const SEARCH ="https://api.spotify.com/v1/search";

function authorize(){
    let url=AUTHORIZE;
    url+="?client_id=" + client_id;
    url+="&response_type=code";
    url+="&redirect_uri="+encodeURI(redirect);
    url+="&show_dialog=true";
    url+="&scope=user-read-private user-read-email playlist-modify-public playlist-modify-private";
    window.location.href=url;
}

function onPageLoad() {
    if (window.location.search.length > 0) {
        handleRedirect();
    } else {
        console.log('No query string found.');
    }
}

function handleRedirect() {
    let code = getCode();
    if (code) {
        fetchAccessToken(code);
        window.history.pushState("", "", redirect);
    }
}

function getCode() {
    let code = null;
    const queryString = window.location.search;
    if (queryString.length > 0) {
        const urlParams = new URLSearchParams(queryString);
        code = urlParams.get('code');
    }
    return code;
}

function fetchAccessToken(code) {
    let body = "grant_type=authorization_code";
    body += "&code=" + encodeURIComponent(code);
    body += "&redirect_uri=" + encodeURIComponent(redirect);
    body += "&client_id=" + encodeURIComponent(client_id);
    body += "&client_secret=" + encodeURIComponent(client_secret);  
    callAuthApi(body);
}

function callAuthApi(body) {
    let xhr = new XMLHttpRequest();
    xhr.open("POST", "https://accounts.spotify.com/api/token", true);
    xhr.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded');
    xhr.setRequestHeader('Authorization', 'Basic ' + btoa(client_id + ":" + client_secret));
    xhr.onload = function() { handleAuthResponse(xhr); };
    xhr.send(body);
}

function refreshAccessToken(){
    refresh_token=localStorage.getItem("refresh_token");
    let body="grant_type=refresh_token";
    body+="&refresh_token=" + refresh_token;
    body+= "&client_id=" + client_id;
    callAuthApi(body);
}

function handleAuthResponse(xhr) {
    if (xhr.status === 200) {
        var data = JSON.parse(xhr.responseText);
        //console.log(data);
        if (data.access_token !== undefined) {
            let access_token = data.access_token;
            at=data.accessToken;
            localStorage.setItem("access_token", access_token);
            console.log("access token:" ,access_token);
            userID=getUserId(access_token);
            accessToken=access_token;
        }
        console.log("user id: ",userID);
        if (data.refresh_token !== undefined) {
            let refresh_token = data.refresh_token;
            localStorage.setItem("refresh_token", refresh_token);
            console.log("refresh token: ",refresh_token);
        }
    } else {
        console.log(xhr.responseText);
        alert(xhr.responseText);
    }
}

function getUserId(accessToken) {
    let xhr = new XMLHttpRequest();
    xhr.open("GET", "https://api.spotify.com/v1/me", false);
    xhr.setRequestHeader('Authorization', 'Bearer ' + accessToken);
    xhr.send(null);
    if (xhr.status === 200) {
        let data = JSON.parse(xhr.responseText);
        return data.id;
    } else {
        console.error('Failed to get user ID:', xhr.status, xhr.responseText);
        return null;
    }
}

function searchSong(){
    var item = document.getElementById("songName").value;
    if (!item) {
        alert("Input a song!");
        return;
    }

    const SEARCH = `https://api.spotify.com/v1/search?q=${encodeURIComponent(item)}&type=track&limit=5`;
    callApi("GET", SEARCH, null, handleSearchResponse);
}

function callApi(method,url,body,callback){
    let xhr = new XMLHttpRequest();
    xhr.open(method, url, true);
    xhr.setRequestHeader('Content-Type', 'application/json');
    xhr.setRequestHeader('Authorization', 'Bearer ' + localStorage.getItem("access_token"));
    xhr.onload = function() {
        if (xhr.status >= 200 && xhr.status < 300) {
            callback(JSON.parse(xhr.responseText));
        } else {
            console.error('API call failed:', xhr.status, xhr.statusText);
            alert('Failed to fetch songs. Please try again.');
        }
    };
    xhr.send(body);
}

function handleSearchResponse(data){
    const resultsDropdown = document.getElementById('resultsDropdown');
    resultsDropdown.innerHTML = '<option value="">Select a song...</option>'; // Reset options

    if (data.tracks && data.tracks.items.length > 0) {
        // Iterate over the tracks and create option elements for each
        data.tracks.items.forEach(track => {
            // Create an option element
            const option = document.createElement('option');
            
            // Set the text content to the track name and artist names
            option.textContent = `${track.name} by ${track.artists.map(artist => artist.name).join(', ')}`;
            
            // Store the track id in the value attribute
            option.value = track.id;
            
            // Append the option to the dropdown
            resultsDropdown.appendChild(option);
        });
    } else {
        const option = document.createElement('option');
        option.textContent = 'No results found.';
        option.value = "";
        resultsDropdown.appendChild(option);
    }
}

let selectedSongIds=[];
function storeSelectedSong() {
    const resultsDropdown = document.getElementById('resultsDropdown');
    const newSelectedIds = Array.from(resultsDropdown.selectedOptions)
                                .map(option => option.value)
                                .filter(id => id);
    newSelectedIds.forEach(id => {
        if (!selectedSongIds.includes(id)) {
            selectedSongIds.push(id);
        }
    });
    console.log('Selected song IDs:', selectedSongIds);
}

function generatePlaylist() {
    if (selectedSongIds.length === 0) {
        alert('Please select at least one song.');
        return;
    }
    getRecommendations(selectedSongIds);
}

function getRecommendations(trackIds) {

    const RECOMMENDATIONS_API = `https://api.spotify.com/v1/recommendations?seed_tracks=${trackIds.join(',')}&limit=30`;
    callApi("GET", RECOMMENDATIONS_API, null, handleRecommendationResponse);
}



function handleRecommendationResponse(data) {
    if(data){
        console.log('Recommendations:', data);
        window.location.href = 'thanks.html';
        const trackUris = data.tracks.map(track => track.uri);
        createPlaylist(playlistName, trackUris);
    } 
    //console.log(accessToken);
    //console.log(userID);
}

function createPlaylist(playlistName,trackUris){
    const accessToken = localStorage.getItem('access_token');
    const userId=getUserId(accessToken);
    //console.log("got it:",accessToken);
    //console.log("got it:",userId);
    //console.log(playlistName);
    fetch(`https://api.spotify.com/v1/users/${userId}/playlists`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + accessToken
        },
        body: JSON.stringify({
            name: playlistName,
            description: 'Playlist created from recommendations',
            public: true
        })
    })
    .then(response => response.json())
    .then(data => {
        const playlistId = data.id;
        addTracksToPlaylist(playlistId, trackUris);
    })
    .catch(error => console.error('Error creating playlist:', error));
}

function addTracksToPlaylist(playlistId,trackUris){
    const accessToken = localStorage.getItem('access_token');
    fetch(`https://api.spotify.com/v1/playlists/${playlistId}/tracks`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + accessToken
        },
        body: JSON.stringify({
            uris: trackUris
        })
    })
    .then(response => response.json())
    .then(data => {
        console.log('Tracks added:', data);
        alert('Playlist created and tracks added!');
    })
    .catch(error => console.error('Error adding tracks to playlist:', error));
}