import Field from "types/Field"
import { Game } from "types/Game";
import Player from "types/Player";

const fields: Field[] = [
    {
        id: "0kq06gugls",
        name: "Holmenkollen",
        holes: Array(18).map((_, index) => ({number: index + 1, par: 3}))
    },
    {
        id: "7j1xc36xqt",
        name: "Charlottenlund",
        holes: Array(9).map((_, index) => ({number: index + 1, par: 3}))
    }
]

const players: Player[] = [
    {
        id: "1",
        name: "Tarald Gåsbakk"
    },
    {
        id: "2",
        name: "Pål Nesbø Lekven"
    },
    {
        id: "2",
        name: "Håvard Snarby"
    },
    {
        id: "3",
        name: "William Tisdal"
    },

]

export function getFields () {
    return fields;
}


export function getPlayers() {
    return players;
}

export function createGame(game: Game) {
    let currentlySavedGames = window.localStorage.getItem("pacoGolfSavedData");
    let newSave = currentlySavedGames ? JSON.parse(currentlySavedGames) as Game[] : [];
    newSave.push(game)
    window.localStorage.setItem("pacoGolfSavedData", JSON.stringify(newSave))
}

export function fetchGame(id: string) {
    console.log("Is Retrieving game")
    let currentlySavedGames = window.localStorage.getItem("pacoGolfSavedData");
    if(currentlySavedGames) {
        return JSON.parse(currentlySavedGames).find((game: Game) => game.id === id)
    } else {
        return undefined
    }
}