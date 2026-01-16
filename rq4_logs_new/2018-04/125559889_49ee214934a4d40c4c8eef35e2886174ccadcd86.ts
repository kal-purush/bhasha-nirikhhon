/* eslint-disable no-undef */
import * as page from './page'
import * as storage from './storage'

async function displayGameList (): Promise<void> {
  try {
    console.log('Displaying list of games')
    // Get and clear current list
    const gamesList: HTMLElement = document.getElementById('gamesList')
    gamesList.innerHTML = ''
    console.log('Cleared gamesList element')

    // Fetch the list of game names
    const gameNames: Array<storage.GameName> = await storage.getGameNames()
    console.log('Fetched list of game names', gameNames)

    // Create the elements
    for (let i = 0; i < gameNames.length; i += 1) {
      const li: HTMLElement = document.createElement('li')
      const text: Text = document.createTextNode(gameNames[i].name)
      li.setAttribute('id', gameNames[i].id)
      li.appendChild(text)

      // Listening for clicks on the game names for opening the save window
      li.addEventListener('click', (): void => {
        const gameId: string = li.id
        sessionStorage.setItem('gameId', gameId)
        console.log(`Set game ID '${gameId}' in sessionStorage`)
        page.displaySavesPage()
      })

      gamesList.appendChild(li)
    }
    console.log('Appended new list of games')
  } catch (err) {
    console.error(`Error while displaying list of games: ${err}`)
  }
}

export {displayGameList}