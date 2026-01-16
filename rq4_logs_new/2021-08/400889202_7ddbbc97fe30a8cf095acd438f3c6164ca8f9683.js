let getInputBox = document.getElementById('getInputBox');
let getInputBtn = document.getElementById('getInputBtn');
let teamsHTML=document.querySelector('#teamOrPlayerDetails .container')
function getInput() {
    let enterValue = getInputBox.value;
    getInputBox.value = '';
    let lowEnterValue = enterValue.toLowerCase();
    searchByName(lowEnterValue)
    teamsHTML.innerHTML=`<div class="d-block m-auto spinner-border" role="status">
    <span class="visually-hidden">Loading...</span>
  </div>`;
}
getInputBtn.onclick = getInput;
getInputBox.addEventListener('keypress',(e)=>{
    if(e.key==='Enter'){
        getInput()
    }
})
// by team name
async function searchByName(lowEnterValue) {
    let fetching = await fetch(`https://www.thesportsdb.com/api/v1/json/1/searchteams.php?t=${lowEnterValue}`);
    let res = fetching.json();
    let data = await res;
    let teams=data.teams;
    let fetching2 = await fetch(`https://www.thesportsdb.com/api/v1/json/1/searchplayers.php?p=${lowEnterValue}`);
    let res2 = fetching2.json();
    let data2 = await res2;
    let players=data2.player;
    console.log(data2)
    teamsHTML.innerHTML=``;
    if(teams!==null && players!==null){
       showTeams(teams);
       showPlayers(players);
    }else if(teams !==null){
        showTeams(teams);
    }else if(players !==null){
        showPlayers(players);
    }else{
        console.log('hello')
    }
    
}
function checkerName(para1,pera2) {
   if(para1 || pera2){
    if(para1.length < pera2){
        return para1;
    }else{
        return pera2;
    }
   }
   
}
function showTeams(teams) {
    for(let team of teams){
        let teamHTML=`
        <div class="row g-3 bg-white mshadow rounded-3 p-3 mt-5">
                <div class="col-12 col-sm-12 col-md-4 col-lg-4  mshadow">
                    <div class="card-body glass rounded-3 center m-3 center">
                        <img src="${team.strTeamBadge}" id="img"
                            alt="">
                    </div>
                </div>
                <div class="col-12 col-sm-12 col-md-6 col-lg-6">
                    <h1 class="torpName">${checkerName(team.strAlternate,team.strTeam)}
                    </h1>
                    <p class="text-secondary" id="description">
                        ${team.strDescriptionEN.slice(0,400)}
                    </p>
                    <a href="https://${team.strWebsite}" class="mybtn rounded-1 mshadow">
                        Details
                    </a>
                </div>
                <div class="col-12 col-sm-12 col-md-2 col-lg-2">
                    <div class="center  flex-sm-row flex-md-column justify-content-start justify-content-md-start">
                        <a href="http://${team.strFacebook}" target="_blank">
                            <div class="social mb-2 me-2 center fb">
                                <i class="fab fa-facebook-f"></i>
                            </div>
                        </a>
                        <a href="http://${team.strInstagram}" target="_blank">
                            <div class="social mb-2 me-2 center instra">
                                <i class="fab fa-instagram"></i>
                            </div>
                        </a>
                        <a href="http://${team.strYoutube}" target="_blank">
                            <div class="social mb-2 me-2 center yt">
                                <i class="fab fa-youtube"></i>
                            </div>
                        </a>
                        <a href="http://${team.strTwitter}" target="_blank">
                            <div class="social mb-2 me-2 center fb">
                                <i class="fab fa-twitter"></i></i>
                            </div>
                        </a>
                    </div>
                </div>
            </div>
        `;
        teamsHTML.innerHTML+=teamHTML;
        
    }
}
function showPlayers(players) {
    for(let player of players){
        let playerHTML=`
        <div class="row g-3 bg-white mshadow rounded-3 p-3 mt-5">
                <div class="col-12 col-sm-12 col-md-4 col-lg-4  mshadow">
                    <div class="card-body glass rounded-3 center m-3 center">
                        <img src="${player.strCutout}" id="img"
                            alt="">
                    </div>
                </div>
                <div class="col-12 col-sm-12 col-md-6 col-lg-6">
                    <h1 class="torpName">${player.strPlayer}
                    </h1>
                    <p class="text-secondary" id="description">
                        ${player.strDescriptionEN.slice(0,400)}
                    </p>
                    <div href="https://${player.strWebsite}" class="mybtn d-inline rounded-1 mshadow">
                        ${player.strNationality}
                    </div>
                </div>
                <div class="col-12 col-sm-12 col-md-2 col-lg-2">
                    <div class="center  flex-sm-row flex-md-column justify-content-start justify-content-md-start">
                        <a href="http://${player.strFacebook}" target="_blank">
                            <div class="social mb-2 me-2 center fb">
                                <i class="fab fa-facebook-f"></i>
                            </div>
                        </a>
                        <a href="http://${player.strInstagram}" target="_blank">
                            <div class="social mb-2 me-2 center instra">
                                <i class="fab fa-instagram"></i>
                            </div>
                        </a>
                    </div>
                </div>
            </div>
        `;
        teamsHTML.innerHTML+=playerHTML;
    }
}