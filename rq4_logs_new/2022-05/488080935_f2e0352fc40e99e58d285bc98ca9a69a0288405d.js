import { React, useState, useEffect } from 'react'
import { Header, Container, SectionBlock, MatchBox } from './styles'
import axios from 'axios'
import { useParams } from "react-router-dom"
import { FaAngleLeft } from 'react-icons/fa';
import { ReturnHome } from "./styles";
import { Link } from 'react-router-dom'

const History = () => {
    const [gameList, setGameList] = useState([]);
    const { id } = useParams();
    const [loaded, setLoaded] = useState(false);


    useEffect(() => {
        axios
            .get("http://localhost:4000/player5Games", { params: { username: id } })
            .then(function (response) {
                if (loaded === false) {
                    setGameList(response.data)
                    setLoaded(true);
                }
            })
            .catch(function (error) {
                console.log(error)
            });
    }, [])
    console.log(gameList);


    function validMap(gameMapId) {
        if (gameMapId === 11) {
            return ("Summoner's Rift")
        }
        else if (gameMapId === 12) {
            return ("Howling Abyss")
        }
    }

    function unixToDate(gameEndTime) {
        const unixTime = gameEndTime;
        var date = new Date(unixTime);
        return (date.toLocaleDateString("en-Us"));
    }

    function secondsToMinutes(gameDuration) {
        const seconds = gameDuration;
        return (Math.floor(seconds / 60))
    }

    function validWin(winStatus) {
        if (winStatus === true) {
            return (<span style={{ color: 'skyblue' }}> Won </span>)
        }
        else {
            return (<span style={{ color: 'Red' }}> Lost </span>)
        }
    }

    function validWinner(winStatus) {
        if (winStatus === true) {
            return ('Won')
        }
        else {
            return ('Lost')
        }
    }

    function getBackgroundColor(winStatus) {
        let color;
        if (winStatus === 'Won') {
            return (color = 'RoyalBlue')
        }
        else {
            return (color = 'Maroon')
        }
    }

    function validSummoner0() {
        for (let i = 0; i < 10; i++) {
            if ((gameList[0].info.participants[i].summonerName).toLowerCase() === (id.toLowerCase())) { // input -> gameList[0].info.participants[i].summonerName
                return (gameList[0].info.participants[i])
            }
        }
    }

    function validSummoner1() {
        for (let i = 0; i < 10; i++) {
            if ((gameList[1].info.participants[i].summonerName).toLowerCase() === (id.toLowerCase())) { // input -> gameList[0].info.participants[i].summonerName
                return (gameList[1].info.participants[i])
            }
        }
    }
    function validSummoner2() {
        for (let i = 0; i < 10; i++) {
            if ((gameList[2].info.participants[i].summonerName).toLowerCase() === (id.toLowerCase())) { // input -> gameList[0].info.participants[i].summonerName
                return (gameList[2].info.participants[i])
            }
        }
    }
    function validSummoner3() {
        for (let i = 0; i < 10; i++) {
            if ((gameList[3].info.participants[i].summonerName).toLowerCase() === (id.toLowerCase())) { // input -> gameList[0].info.participants[i].summonerName
                return (gameList[3].info.participants[i])
            }
        }
    }
    function validSummoner4() {
        for (let i = 0; i < 10; i++) {
            if ((gameList[4].info.participants[i].summonerName).toLowerCase() === (id.toLowerCase())) { // input -> gameList[0].info.participants[i].summonerName
                return (gameList[4].info.participants[i])
            }
        }
    }
    function validSummoner5() {
        for (let i = 0; i < 10; i++) {
            if ((gameList[5].info.participants[i].summonerName).toLowerCase() === (id.toLowerCase())) { // input -> gameList[0].info.participants[i].summonerName
                return (gameList[5].info.participants[i])
            }
        }
    }
    function validSummoner6() {
        for (let i = 0; i < 10; i++) {
            if ((gameList[6].info.participants[i].summonerName).toLowerCase() === (id.toLowerCase())) { // input -> gameList[0].info.participants[i].summonerName
                return (gameList[6].info.participants[i])
            }
        }
    }

    function validSpell(spell) {
        switch (spell) {
            case 21:
                return (<img src="https://ddragon.leagueoflegends.com/cdn/12.8.1/img/spell/SummonerBarrier.png" alt="" />)
            case 1:
                return (<img src="https://ddragon.leagueoflegends.com/cdn/12.8.1/img/spell/SummonerBoost.png" alt="" />)
            case 14:
                return (<img src="https://ddragon.leagueoflegends.com/cdn/12.8.1/img/spell/SummonerDot.png" alt="" />)
            case 3:
                return (<img src="https://ddragon.leagueoflegends.com/cdn/12.8.1/img/spell/SummonerExhaust.png" alt="" />)
            case 4:
                return (<img src="https://ddragon.leagueoflegends.com/cdn/12.8.1/img/spell/SummonerFlash.png" alt="" />)
            case 6:
                return (<img src="https://ddragon.leagueoflegends.com/cdn/12.8.1/img/spell/SummonerHaste.png" alt="" />)
            case 7:
                return (<img src="https://ddragon.leagueoflegends.com/cdn/12.8.1/img/spell/SummonerHeal.png" alt="" />)
            case 13:
                return (<img src="https://ddragon.leagueoflegends.com/cdn/12.8.1/img/spell/SummonerMana.png" alt="" />)
            case 11:
                return (<img src="https://ddragon.leagueoflegends.com/cdn/12.8.1/img/spell/SummonerSmite.png" alt="" />)
            case 12:
                return (<img src="https://ddragon.leagueoflegends.com/cdn/12.8.1/img/spell/SummonerTeleport.png" alt="" />)
            case 32:
                return (<img src="https://ddragon.leagueoflegends.com/cdn/12.8.1/img/spell/SummonerSnowball.png" alt="" />)
            case 0:
                return (false)
            default: break;
        }
    }

    if (loaded === "false") {
        return (loaded);
    } else if (loaded && gameList.length !== 0) {
        return (
            <div class="container">
                <div class="matchHistory">
                    <Header>
                        <h1>Match History</h1>
                    </Header>
                </div>
                <div class="matchGames">
                    <div style={{ backgroundColor: `${getBackgroundColor(validWinner(validSummoner0().win))}` }} class="game1" ></div>
                    <div class="ally-team1">
                        <ul>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[0].info.participants[0].championName + ".png"} />{gameList[0].info.participants[0].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[0].info.participants[1].championName + ".png"} />{gameList[0].info.participants[1].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[0].info.participants[2].championName + ".png"} />{gameList[0].info.participants[2].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[0].info.participants[3].championName + ".png"} />{gameList[0].info.participants[3].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[0].info.participants[4].championName + ".png"} />{gameList[0].info.participants[4].summonerName}</li>
                        </ul>

                    </div>
                    <div class="enemy-team1">
                        <ul>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[0].info.participants[5].championName + ".png"} />{gameList[0].info.participants[5].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[0].info.participants[6].championName + ".png"} />{gameList[0].info.participants[6].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[0].info.participants[7].championName + ".png"} />{gameList[0].info.participants[7].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[0].info.participants[8].championName + ".png"} />{gameList[0].info.participants[8].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[0].info.participants[9].championName + ".png"} />{gameList[0].info.participants[9].summonerName}</li>
                        </ul>
                    </div>
                    <div class="items1">
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner0().item0 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner0().item1 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner0().item2 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner0().item3 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner0().item4 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner0().item5 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner0().item6 + ".png"} />
                    </div>
                    <div class="kda1">
                        <ul>
                            <li>{validSummoner0().kills} / {validSummoner0().deaths} / {validSummoner0().assists}</li>
                            <li> {((validSummoner0().kills + validSummoner0().assists) / validSummoner0().deaths).toFixed(2)} KDA </li>
                            <li> {validSummoner0().totalMinionsKilled + validSummoner0().neutralMinionsKilled} CS</li>
                            <li> {validSummoner0().visionScore} vision</li>
                        </ul>
                    </div>

                    <div class="champion1">
                        <div className="row1">
                            <div className="column">
                                <img className="imageChamp1" alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + validSummoner0().championName + ".png"} />
                            </div>
                            <div className="column1">
                                <span className="ImageSpell1">{validSpell(validSummoner0().summoner1Id)} {validSpell(validSummoner0().summoner2Id)}</span>
                            </div>
                        </div>
                    </div>
                    <div class="winloss1">
                        <ul>
                            <li>{validMap(gameList[0].info.mapId)}</li>
                            <li>{unixToDate(gameList[0].info.gameEndTimestamp)}</li>
                            <li>{secondsToMinutes(gameList[0].info.gameDuration)} mins</li>
                            <li>{(validWin(validSummoner0().win))}</li>
                        </ul>
                    </div>

                    <div style={{ backgroundColor: `${getBackgroundColor(validWinner(validSummoner1().win))}` }} class="game2"></div>
                    {console.log((validWin(validSummoner1().win)))}
                    <div class="winloss2">
                        <ul>
                            <li>{validMap(gameList[1].info.mapId)}</li>

                            <li>{unixToDate(gameList[1].info.gameEndTimestamp)}</li>

                            <li>{secondsToMinutes(gameList[1].info.gameDuration)} mins</li>

                            <li>{validWin(validSummoner1().win)}</li>
                        </ul>
                    </div>
                    <div class="champion2">
                        <div className="row1">
                            <div className="column">
                                <img className="imageChamp1" alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + validSummoner1().championName + ".png"} />
                            </div>
                            <div className="column1">
                                <span className="ImageSpell1">{validSpell(validSummoner1().summoner1Id)} {validSpell(validSummoner1().summoner2Id)}</span>
                            </div>
                        </div>
                    </div>
                    <div class="kda2">
                        <ul>
                            <li>{validSummoner1().kills} / {validSummoner1().deaths} / {validSummoner1().assists}</li>
                            <li> {((validSummoner1().kills + validSummoner1().assists) / validSummoner1().deaths).toFixed(2)} KDA </li>
                            <li> {validSummoner1().totalMinionsKilled + validSummoner1().neutralMinionsKilled} CS</li>
                            <li> {validSummoner1().visionScore} vision</li>
                        </ul>
                    </div>
                    <div class="items2">
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner1().item0 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner1().item1 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner1().item2 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner1().item3 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner1().item4 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner1().item5 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner1().item6 + ".png"} />
                    </div>
                    <div class="ally-team2">
                        <ul>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[1].info.participants[0].championName + ".png"} />{gameList[1].info.participants[0].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[1].info.participants[1].championName + ".png"} />{gameList[1].info.participants[1].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[1].info.participants[2].championName + ".png"} />{gameList[1].info.participants[2].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[1].info.participants[3].championName + ".png"} />{gameList[1].info.participants[3].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[1].info.participants[4].championName + ".png"} />{gameList[1].info.participants[4].summonerName}</li>
                        </ul>
                    </div>
                    <div class="enemy-team2">
                        <ul>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[1].info.participants[5].championName + ".png"} />{gameList[1].info.participants[5].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[1].info.participants[6].championName + ".png"} />{gameList[1].info.participants[6].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[1].info.participants[7].championName + ".png"} />{gameList[1].info.participants[7].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[1].info.participants[8].championName + ".png"} />{gameList[1].info.participants[8].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[1].info.participants[9].championName + ".png"} />{gameList[1].info.participants[9].summonerName}</li>
                        </ul>
                    </div>
                    <div style={{ backgroundColor: `${getBackgroundColor(validWinner(validSummoner2().win))}` }} class="game3"></div>
                    <div class="winloss3">
                        <ul>
                            <li>{validMap(gameList[2].info.mapId)}</li>
                            <li>{unixToDate(gameList[2].info.gameEndTimestamp)}</li>
                            <li>{secondsToMinutes(gameList[2].info.gameDuration)} mins</li>
                            <li>{validWin(validSummoner2().win)}</li>
                        </ul>
                    </div>
                    <div class="champion3">
                        <div className="row1">
                            <div className="column">
                                <img className="imageChamp1" alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + validSummoner2().championName + ".png"} />
                            </div>
                            <div className="column1">
                                <span>{validSpell(validSummoner2().summoner1Id)} {validSpell(validSummoner2().summoner2Id)}</span>
                            </div>
                        </div>
                    </div>
                    <div class="kda3">
                        <ul>
                            <li>{validSummoner2().kills} / {validSummoner2().deaths} / {validSummoner2().assists}</li>
                            <li> {((validSummoner2().kills + validSummoner2().assists) / validSummoner2().deaths).toFixed(2)} KDA </li>
                            <li> {validSummoner2().totalMinionsKilled + validSummoner2().neutralMinionsKilled} CS</li>
                            <li> {validSummoner2().visionScore} vision</li>
                        </ul>
                    </div>
                    <div class="items3">
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner2().item0 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner2().item1 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner2().item2 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner2().item3 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner2().item4 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner2().item5 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner2().item6 + ".png"} />
                    </div>

                    <div class="ally-team3">
                        <ul>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[2].info.participants[0].championName + ".png"} />{gameList[2].info.participants[0].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[2].info.participants[1].championName + ".png"} />{gameList[2].info.participants[1].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[2].info.participants[2].championName + ".png"} />{gameList[2].info.participants[2].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[2].info.participants[3].championName + ".png"} />{gameList[2].info.participants[3].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[2].info.participants[4].championName + ".png"} />{gameList[2].info.participants[4].summonerName}</li>
                        </ul>
                    </div>
                    <div class="enemy-team3">
                        <ul>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[2].info.participants[5].championName + ".png"} />{gameList[2].info.participants[5].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[2].info.participants[6].championName + ".png"} />{gameList[2].info.participants[6].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[2].info.participants[7].championName + ".png"} />{gameList[2].info.participants[7].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[2].info.participants[8].championName + ".png"} />{gameList[2].info.participants[8].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[2].info.participants[9].championName + ".png"} />{gameList[2].info.participants[9].summonerName}</li>
                        </ul>
                    </div>
                    <div style={{ backgroundColor: `${getBackgroundColor(validWinner(validSummoner3().win))}` }} class="game4"></div>
                    <div class="winloss4">
                        <ul>
                            <li>{validMap(gameList[3].info.mapId)}</li>
                            <li>{unixToDate(gameList[3].info.gameEndTimestamp)}</li>
                            <li>{secondsToMinutes(gameList[3].info.gameDuration)} mins</li>
                            <li>{validWin(validSummoner3().win)}</li>
                        </ul>
                    </div>
                    <div class="champion4">
                        <div className="row1">
                            <div className="column">
                                <img className="imageChamp1" alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + validSummoner3().championName + ".png"} />
                            </div>
                            <div className="column1">
                                <span>{validSpell(validSummoner3().summoner1Id)} {validSpell(validSummoner3().summoner2Id)}</span>
                            </div>
                        </div>
                    </div>
                    <div class="kda4">
                        <ul>
                            <li>{validSummoner3().kills} / {validSummoner3().deaths} / {validSummoner3().assists}</li>
                            <li> {((validSummoner3().kills + validSummoner3().assists) / validSummoner3().deaths).toFixed(2)} KDA </li>
                            <li> {validSummoner3().totalMinionsKilled + validSummoner3().neutralMinionsKilled} CS</li>
                            <li> {validSummoner3().visionScore} vision</li>
                        </ul>
                    </div>
                    <div class="items4">
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner3().item0 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner3().item1 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner3().item2 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner3().item3 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner3().item4 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner3().item5 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner3().item6 + ".png"} />
                    </div>
                    <div class="ally-team4">
                        <ul>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[3].info.participants[0].championName + ".png"} />{gameList[3].info.participants[0].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[3].info.participants[1].championName + ".png"} />{gameList[3].info.participants[1].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[3].info.participants[2].championName + ".png"} />{gameList[3].info.participants[2].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[3].info.participants[3].championName + ".png"} />{gameList[3].info.participants[3].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[3].info.participants[4].championName + ".png"} />{gameList[3].info.participants[4].summonerName}</li>
                        </ul>
                    </div>
                    <div class="enemy-team4">
                        <ul>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[3].info.participants[5].championName + ".png"} />{gameList[3].info.participants[5].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[3].info.participants[6].championName + ".png"} />{gameList[3].info.participants[6].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[3].info.participants[7].championName + ".png"} />{gameList[3].info.participants[7].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[3].info.participants[8].championName + ".png"} />{gameList[3].info.participants[8].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[3].info.participants[9].championName + ".png"} />{gameList[3].info.participants[9].summonerName}</li>
                        </ul>
                    </div>
                    <div style={{ backgroundColor: `${getBackgroundColor(validWinner(validSummoner4().win))}` }} class="game5"></div>
                    <div class="winloss5">
                        <ul>
                            <li>{validMap(gameList[4].info.mapId)}</li>
                            <li>{unixToDate(gameList[4].info.gameEndTimestamp)}</li>
                            <li>{secondsToMinutes(gameList[4].info.gameDuration)} mins</li>
                            <li>{validWin(validSummoner4().win)}</li>
                        </ul>
                    </div>
                    <div class="champion5">
                        <div className="row1">
                            <div className="column">
                                <img className="imageChamp1" alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + validSummoner4().championName + ".png"} />
                            </div>
                            <div className="column1">
                                <span>{validSpell(validSummoner4().summoner1Id)} {validSpell(validSummoner4().summoner2Id)}</span>
                            </div>
                        </div>

                    </div>
                    <div class="kda5">
                        <ul>
                            <li>{validSummoner4().kills} / {validSummoner4().deaths} / {validSummoner4().assists}</li>
                            <li> {((validSummoner4().kills + validSummoner4().assists) / validSummoner4().deaths).toFixed(2)} KDA </li>
                            <li> {validSummoner4().totalMinionsKilled + validSummoner4().neutralMinionsKilled} CS</li>
                            <li> {validSummoner4().visionScore} vision</li>
                        </ul>
                    </div>
                    <div class="items5">
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner4().item0 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner4().item1 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner4().item2 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner4().item3 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner4().item4 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner4().item5 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner4().item6 + ".png"} />

                    </div>
                    <div class="ally-team5">
                        <ul>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[4].info.participants[0].championName + ".png"} />{gameList[4].info.participants[0].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[4].info.participants[1].championName + ".png"} />{gameList[4].info.participants[1].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[4].info.participants[2].championName + ".png"} />{gameList[4].info.participants[2].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[4].info.participants[3].championName + ".png"} />{gameList[4].info.participants[3].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[4].info.participants[4].championName + ".png"} />{gameList[4].info.participants[4].summonerName}</li>
                        </ul>
                    </div>
                    <div class="enemy-team5">
                        <ul>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[4].info.participants[5].championName + ".png"} />{gameList[4].info.participants[5].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[4].info.participants[6].championName + ".png"} />{gameList[4].info.participants[6].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[4].info.participants[7].championName + ".png"} />{gameList[4].info.participants[7].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[4].info.participants[8].championName + ".png"} />{gameList[4].info.participants[8].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[4].info.participants[9].championName + ".png"} />{gameList[4].info.participants[9].summonerName}</li>
                        </ul>
                    </div>
                    <div style={{ backgroundColor: `${getBackgroundColor(validWinner(validSummoner5().win))}` }} class="game6"></div>
                    <div class="winloss6">
                        <ul>
                            <li>{validMap(gameList[5].info.mapId)}</li>
                            <li>{unixToDate(gameList[5].info.gameEndTimestamp)}</li>
                            <li>{secondsToMinutes(gameList[5].info.gameDuration)} mins</li>
                            <li>{validWin(validSummoner5().win)}</li>
                        </ul>
                    </div>
                    <div class="champion6">
                        <div className="row1">
                            <div className="column">
                                <img className="imageChamp1" alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + validSummoner5().championName + ".png"} />
                            </div>
                            <div className="column1">
                                <span>{validSpell(validSummoner5().summoner1Id)} {validSpell(validSummoner5().summoner2Id)}</span>
                            </div>
                        </div>

                    </div>
                    <div class="kda6">
                        <ul>
                            <li>{validSummoner5().kills} / {validSummoner5().deaths} / {validSummoner5().assists}</li>
                            <li> {((validSummoner5().kills + validSummoner5().assists) / validSummoner5().deaths).toFixed(2)} KDA </li>
                            <li> {validSummoner5().totalMinionsKilled + validSummoner5().neutralMinionsKilled} CS</li>
                            <li> {validSummoner5().visionScore} vision</li>
                        </ul>
                    </div>
                    <div class="items6">
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner5().item0 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner5().item1 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner5().item2 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner5().item3 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner5().item4 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner5().item5 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner5().item6 + ".png"} />

                    </div>
                    <div class="ally-team6">
                        <ul>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[5].info.participants[0].championName + ".png"} />{gameList[5].info.participants[0].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[5].info.participants[1].championName + ".png"} />{gameList[5].info.participants[1].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[5].info.participants[2].championName + ".png"} />{gameList[5].info.participants[2].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[5].info.participants[3].championName + ".png"} />{gameList[5].info.participants[3].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[5].info.participants[4].championName + ".png"} />{gameList[5].info.participants[4].summonerName}</li>
                        </ul>
                    </div>
                    <div class="enemy-team6">
                        <ul>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[5].info.participants[5].championName + ".png"} />{gameList[5].info.participants[5].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[5].info.participants[6].championName + ".png"} />{gameList[5].info.participants[6].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[5].info.participants[7].championName + ".png"} />{gameList[5].info.participants[7].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[5].info.participants[8].championName + ".png"} />{gameList[5].info.participants[8].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[5].info.participants[9].championName + ".png"} />{gameList[5].info.participants[9].summonerName}</li>
                        </ul>
                    </div>
                    <div style={{ backgroundColor: `${getBackgroundColor(validWinner(validSummoner6().win))}` }} class="game7"></div>
                    <div class="winloss7">
                        <ul>
                            <li>{validMap(gameList[6].info.mapId)}</li>
                            <li>{unixToDate(gameList[6].info.gameEndTimestamp)}</li>
                            <li>{secondsToMinutes(gameList[6].info.gameDuration)} mins</li>
                            <li>{validWin(validSummoner6().win)}</li>
                        </ul>
                    </div>
                    <div class="champion7">
                        <div className="row1">
                            <div className="column">
                                <img className="imageChamp1" alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + validSummoner6().championName + ".png"} />
                            </div>
                            <div className="column1">
                                <span>{validSpell(validSummoner6().summoner1Id)} {validSpell(validSummoner6().summoner2Id)}</span>
                            </div>
                        </div>

                    </div>
                    <div class="kda7">
                        <ul>
                            <li>{validSummoner6().kills} / {validSummoner6().deaths} / {validSummoner6().assists}</li>
                            <li> {((validSummoner6().kills + validSummoner6().assists) / validSummoner6().deaths).toFixed(2)} KDA </li>
                            <li> {validSummoner6().totalMinionsKilled + validSummoner6().neutralMinionsKilled} CS</li>
                            <li> {validSummoner6().visionScore} vision</li>
                        </ul>
                    </div>
                    <div class="items7">
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner6().item0 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner6().item1 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner6().item2 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner6().item3 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner6().item4 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner6().item5 + ".png"} />
                        <img alt="" src={"https://ddragon.leagueoflegends.com/cdn/12.8.1/img/item/" + validSummoner6().item6 + ".png"} />

                    </div>
                    <div class="ally-team7">
                        <ul>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[6].info.participants[0].championName + ".png"} />{gameList[6].info.participants[0].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[6].info.participants[1].championName + ".png"} />{gameList[6].info.participants[1].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[6].info.participants[2].championName + ".png"} />{gameList[6].info.participants[2].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[6].info.participants[3].championName + ".png"} />{gameList[6].info.participants[3].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[6].info.participants[4].championName + ".png"} />{gameList[6].info.participants[4].summonerName}</li>
                        </ul>
                    </div>
                    <div class="enemy-team7">
                        <ul>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[6].info.participants[5].championName + ".png"} />{gameList[6].info.participants[5].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[6].info.participants[6].championName + ".png"} />{gameList[6].info.participants[6].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[6].info.participants[7].championName + ".png"} />{gameList[6].info.participants[7].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[6].info.participants[8].championName + ".png"} />{gameList[6].info.participants[8].summonerName}</li>
                            <li><img alt="" src={"http://ddragon.leagueoflegends.com/cdn/12.8.1/img/champion/" + gameList[6].info.participants[9].championName + ".png"} />{gameList[6].info.participants[9].summonerName}</li>
                        </ul>
                    </div>
                </div>
                <div class="returnHome">
                    <Link to={`/summoner/${id}`}>
                        <ReturnHome>
                            <FaAngleLeft size={30} color="#FFFFF" />
                            <span> BACK</span>
                        </ReturnHome>
                    </Link>
                </div>
            </div >
        )
    }
};

export default History



/* <Header>
    <h1>Match History</h1>
</Header>
&nbsp;
<Container>
    {
        gameList.map((gameData) =>
            <>
            
                <MatchBox>
                    <ul>
                        {gameData.info.participants.slice(0, 5).map((data, participantIndex) =>
                            <li> {data.summonerName}  </li>
                        )}
                    </ul>
                    <ul>
                        {gameData.info.participants.slice(5, 11).map((data, participantIndex) =>
                            <li> {data.summonerName} </li>
                        )}
                    </ul>
                </MatchBox>

            </>
        )}
</Container>

</> */