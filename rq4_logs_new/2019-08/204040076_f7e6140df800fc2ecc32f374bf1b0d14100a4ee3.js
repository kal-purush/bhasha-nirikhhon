const newGame = () => {
  let id = 0
  const game = []
  const playArea = document.getElementById('play-area')
  const difficulty = document.querySelector('select')
  if (difficulty.value === 'easy') {
    playArea.classList.add('easy')
    for (let i = 0; i < 81; i++) {
      let newSquare = document.createElement('div')
      newSquare.classList.add('game-square')
      newSquare.id = id
      game.push(newSquare)
      id++
    }

    game.forEach(element => {
      console.log(element)
      playArea.append(element)
    })
  }

  console.log(game)
}


newGame()