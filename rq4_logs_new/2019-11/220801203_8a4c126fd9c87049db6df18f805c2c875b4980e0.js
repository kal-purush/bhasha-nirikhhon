class Form {
  constructor() {
    this.height = 0
    this.width = 0
    this.mines = 0
    this.generateForm()
  }
  inputChange = e => {
    if (isNaN(e.target.value)) {
      setTimeout(() => {
        e.target.value = ""
      }, 1000)
    } else {
      this[e.target.name] = parseInt(e.target.value)
    }
  }

  formSubmit = e => {
    e.preventDefault()
    if (this.height && this.width && this.mines) {
      const form = document.querySelector('.form')
      form.innerHTML = ""
      new Saper(this.width, this.height, this.mines)

      e.target.querySelectorAll('input').forEach(element => {
        element.disabled = true;
      });
      e.target.querySelector('button').disabled = true;
    } else {
      alert("Uzupełnij wszystki pola!")
    }
  }

  generateForm = () => {
    // Generowanie formularza UI
    const divForm = document.querySelector('.form')
    const form = document.createElement('form')

    const divHeight = document.createElement('div')
    const labelHeight = document.createElement('label')
    labelHeight.textContent = "Height: "
    const inputHeigth = document.createElement('input')
    inputHeigth.setAttribute('name', 'height')
    inputHeigth.setAttribute('autocomplete', 'off')
    inputHeigth.addEventListener('change', this.inputChange)
    divHeight.appendChild(labelHeight)
    divHeight.appendChild(inputHeigth)

    const divWidth = document.createElement('div')
    const labelWidth = document.createElement('label')
    labelWidth.textContent = "Width: "
    const inputWidth = document.createElement('input')
    inputWidth.setAttribute('name', 'width')
    inputWidth.setAttribute('autocomplete', 'off')
    inputWidth.addEventListener('change', this.inputChange)
    divWidth.appendChild(labelWidth)
    divWidth.appendChild(inputWidth)

    const divMines = document.createElement('div')
    const labelMines = document.createElement('label')
    labelMines.textContent = "Mines: "
    const inputMines = document.createElement('input')
    inputMines.setAttribute('name', 'mines')
    inputMines.setAttribute('autocomplete', 'off')
    inputMines.addEventListener('change', this.inputChange)
    divMines.appendChild(labelMines)
    divMines.appendChild(inputMines)

    const btnSubmit = document.createElement('button')
    btnSubmit.textContent = "Start"
    btnSubmit.classList.add('btn-sumbit')

    form.append(divHeight)
    form.append(divWidth)
    form.append(divMines)
    form.append(btnSubmit)
    form.addEventListener('submit', this.formSubmit)

    divForm.append(form)
  }
}

class Saper {
  constructor(width, height, mines) {
    this.width = width
    this.height = height
    this.mines = mines
    this.field = []
    this.minesFields = []
    this.mayBombs = []
    this.time = 0
    this.prepareGame()
    this.timer;
    this.lose = false;
  }
  setMines = () => {
    // Wypełnienie palnszy zerami
    for (let i = 0; i < this.height; i++) {
      this.field[i] = []
      for (let j = 0; j < this.width; j++) {
        this.field[i][j] = 0
      }
    }
    // Wypełnienie planszy bombai
    let countOfMines = this.mines
    for (; countOfMines > 0; countOfMines--) {
      const x = Math.floor(Math.random() * this.width);
      const y = Math.floor(Math.random() * this.height);
      if (this.field[y][x] === "B") countOfMines++
      else {
        this.field[y][x] = "B";
        this.minesFields.push([x, y])
      }
    }

    // Wypełnienie planszy informacjami o pozycji bomb
    for (let i = 0; i < this.height; i++) {
      for (let j = 0; j < this.width; j++) {
        if (this.field[i][j] !== "B") {
          try {
            if (this.field[i][j + 1] === "B") {
              this.field[i][j] += 1
            }
          } catch (error) { }

          try {
            if (this.field[i][j - 1] === "B") {
              this.field[i][j] += 1
            }
          } catch (error) { }

          try {
            if (this.field[i + 1][j] === "B") {
              this.field[i][j] += 1
            }
          } catch (error) { }

          try {
            if (this.field[i + 1][j + 1] === "B") {
              this.field[i][j] += 1
            }
          } catch (error) { }

          try {
            if (this.field[i + 1][j - 1] === "B") {
              this.field[i][j] += 1
            }
          } catch (error) { }

          try {
            if (this.field[i - 1][j + 1] === "B") {
              this.field[i][j] += 1
            }
          } catch (error) { }

          try {
            if (this.field[i - 1][j - 1] === "B") {
              this.field[i][j] += 1
            }
          } catch (error) { }

          try {
            if (this.field[i - 1][j] === "B") {
              this.field[i][j] += 1
            }
          } catch (error) { }
        }
      }
    }
    console.log(this.field);
  }

  setArea = () => {
    const container = document.querySelector('.area')
    // Ustawnienie headeru z iloscia bomb
    const minesHeader = document.createElement('h2')
    minesHeader.textContent = "Pozostało bomb: "
    const spanMines = document.createElement('span')
    spanMines.classList.add('minesCount')
    spanMines.textContent = this.mines
    minesHeader.appendChild(spanMines)
    container.appendChild(minesHeader)
    // Ustawnienie headeru z mierzeniem czasu
    const timerHeader = document.createElement('h3')
    timerHeader.textContent = "Grasz: "
    const spanTimer = document.createElement('span')
    spanTimer.classList.add('time')
    spanTimer.textContent = `${this.time}s`
    this.timer = setInterval(() => {
      this.time++
      spanTimer.textContent = `${this.time}s`
    }, 1000)
    timerHeader.appendChild(spanTimer)
    container.appendChild(timerHeader)

    // Stworzenie areny do sapera 
    const saperContainer = document.createElement('div')
    saperContainer.classList.add('saper')
    saperContainer.style.width = `${this.width * 20}px`
    saperContainer.style.height = `${this.height * 20}px`
    for (let i = 0; i < this.height; i++) {
      for (let j = 0; j < this.width; j++) {
        const div = document.createElement('div')
        div.classList.add('field')
        div.setAttribute('data-row', i)
        div.setAttribute('data-col', j)
        //div.textContent = this.field[i][j]
        div.addEventListener('click', this.saperClick)
        div.addEventListener('contextmenu', this.saperClick)
        saperContainer.appendChild(div)
      }
    }
    container.appendChild(saperContainer)
  }

  discover = (x, y) => {
    try {
      const div = document.querySelector(`[data-row="${y}"][data-col="${x}"]`)
      if (this.field[y][x] === "B") {
        div.classList.add('bomb')
      } else {
        div.textContent = this.field[y][x]
        div.classList.add('seek')
      }

      div.removeEventListener('click', this.saperClick)
    } catch (err) { }
  }

  zeroSquare = (x, y) => {
    this.discover(x, y)
    this.discover(x + 1, y)
    this.discover(x - 1, y)
    this.discover(x + 1, y + 1)
    this.discover(x - 1, y + 1)
    this.discover(x, y + 1)
    this.discover(x + 1, y - 1)
    this.discover(x - 1, y - 1)
    this.discover(x, y - 1)
  }

  saperZero = (x, y) => {
    y = parseInt(y)
    x = parseInt(x)
    try {
      if (this.field[y][x + 1] === 0) {
        this.zeroSquare(x + 1, y)
      }
    } catch (error) { }
    try {
      if (this.field[y][x - 1] === 0) {
        this.zeroSquare(x - 1, y)
      }
    } catch (error) { }
    try {
      if (this.field[y + 1][x - 1] === 0) {
        this.zeroSquare(x - 1, y + 1)
      }
    } catch (error) { }
    try {
      if (this.field[y + 1][x + 1] === 0) {
        this.zeroSquare(x + 1, y + 1)
      }
    } catch (error) { }
    try {
      if (this.field[y + 1][x] === 0) {
        this.zeroSquare(x, y + 1)
      }
    } catch (error) { }
    try {
      if (this.field[y - 1][x - 1] === 0) {
        this.zeroSquare(x - 1, y - 1)
      }
    } catch (error) { }
    try {
      if (this.field[y - 1][x + 1] === 0) {
        this.zeroSquare(x + 1, y - 1)
      }
    } catch (error) { }
    try {
      if (this.field[y - 1][x] === 0) {
        this.zeroSquare(x, y - 1)
      }
    } catch (error) { }

    this.zeroSquare(x, y)
  }

  offSaper = () => {
    for (const el of this.minesFields) {
      this.discover(el[0], el[1])
    }
    const divs = document.querySelectorAll('.saper > div')
    for (const div of divs) {
      div.removeEventListener('click', this.saperClick)
      div.removeEventListener('contextmenu', this.saperClick)
    }
    clearInterval(this.timer)
  }

  gameOver = () => {
    const saper = document.querySelector('.saper')
    saper.classList.add('gameover')
    this.offSaper()
    new Scores()
  }

  checkBombs = () => {
    for (const bomb of this.mayBombs) {
      if (this.lose) break
      for (let i = 0; this.minesFields.length > i; i++) {
        const obj = JSON.stringify({ x: `${this.minesFields[i][0]}`, y: `${this.minesFields[i][1]}` })
        if (obj !== bomb) {
          this.lose = true;
          this.gameOver()
          break
        }
      }
    }
  }

  gameWon = () => {
    this.offSaper()
    new Scores(this.time, this.width, this.height, this.minesFields.length)
  }

  saperClick = (e) => {
    e.preventDefault()
    const { row, col } = e.target.dataset
    if (e.button === 0) {
      if (this.field[row][col] === "B") {
        this.discover(col, row)
        const div = document.querySelector(`[data-row="${row}"][data-col="${col}"]`)
        div.classList.add('bomb')
        this.gameOver()
      } else if (this.field[row][col] === 0) {
        this.saperZero(col, row)
      } else {
        e.target.classList.add('seek')
        e.target.removeEventListener('click', this.saperClick)
        e.target.removeEventListener('contextmenu', this.saperClick)
      }
      e.target.textContent = this.field[row][col]
    }
    if (e.button === 2) {
      const span = document.querySelector('.minesCount')
      if (e.target.classList.contains('mayBomb')) {
        e.target.classList.remove('mayBomb')
        const obj = JSON.stringify({ x: col, y: row })
        const index = this.mayBombs.indexOf(obj)
        this.mayBombs.splice(index, 1)
        this.mines++
      } else if (this.mines > 0) {
        e.target.classList.add('mayBomb')
        this.mayBombs.push(JSON.stringify({ x: col, y: row }))
        this.mines--;
        if (this.mines === 0) {
          this.checkBombs()
          if (!this.lose) {
            this.gameWon()
            console.log("Wygrales!");
          }
        }
      }
      span.textContent = this.mines
    }
  }

  prepareGame = () => {
    this.setMines()
    this.setArea()
  }
}

class Scores {
  constructor(time, width, height, bombsCount) {
    this.won = time ? true : false;
    this.stats = {}
    //this.getCookies()
    this.addNewScore(time, width, height, bombsCount)
    //this.generateTable()
    //this.setCookies()
    this.restartButton()

  }
  restartButton = () => {
    const score = document.querySelector('.scores')
    const h3 = document.createElement('h3')
    h3.textContent = this.won ? "You won!" : "You lost..."
    score.appendChild(h3)
    const btn = document.createElement('button')
    btn.textContent = "Restart"
    btn.addEventListener('click', () => {
      const form = document.querySelector('.form')
      form.innerHTML = ""
      const area = document.querySelector('.area')
      area.innerHTML = ""
      const scores = document.querySelector('.scores')
      scores.innerHTML = ""
      new Form()
    })
    score.append(btn)
  }

  addNewScore = (time, width, height, bombsCount) => {

  }
}

new Form()