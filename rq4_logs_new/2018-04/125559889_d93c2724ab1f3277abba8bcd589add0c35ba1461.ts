/* eslint-disable no-undef */
const Store = require('electron-store')
const dataJSON = new Store({name: 'data', cwd: 'storage'})

// data.json interface
interface BossInfo {
  name: string
}

interface Bosses {
  [x: string]: BossInfo
}

interface Game {
  name: string,
  bosses: Bosses
}

interface Games {
  [x: string]: Game
}
// Interfaces for file
interface GameName {
  id: string,
  name: string
}

// Functions
async function getGameNames (): Promise<Array<GameName>> {
  try {
    console.log('Fetching game names')
    const gameInfo: Games = dataJSON.get(`games`)
    const gameNames: Array<GameName> = []

    console.log('Putting names and IDs into an array')
    Object.entries(gameInfo).forEach(([key, value]): void => {
      const gameName: GameName = {
        id: key,
        name: value.name
      }
      gameNames.push(gameName)
    })

    return gameNames
  } catch (err) {
    console.error('Error while fetching game info:', err)
  }
}

async function getGameInfo (gameId: string): Promise<Game> {
  try {
    const gameInfo: Game = dataJSON.get(`games.${gameId}`)
    console.log('Fetched game info', gameInfo)
    return gameInfo
  } catch (err) {
    console.error('Error while getting game info:', err)
  }
}

export {BossInfo, Bosses, Game, Games, GameName, getGameNames, getGameInfo}