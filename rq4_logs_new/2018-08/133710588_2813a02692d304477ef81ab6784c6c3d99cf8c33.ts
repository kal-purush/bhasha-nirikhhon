import { Store } from './store'
import { Tet } from './tet'

/** Represents our game board and interface */
export class Game {
  // Public Vars
  /**
   * Since the Tetris standard is to have 10 horizontal blocks by 16 vertical
   * blocks, this is a constant set to 16.
   */
  static readonly BOARD_ROW_NUM: number = 16
  /**
   * Since the Tetris standard is to have 10 horizontal blocks by 16 vertical
   * blocks, this is a constant set to 10.
   */
  static readonly BOARD_COL_NUM: number = 10
  /** Developer Mode (when enabled/true, test cases can be ran via keybinds) */
  devModeOn: boolean
  /**
   * If true, we want to create a new Tet at the beginning of the loop
   * interval.
   *
   * Defaults as true since we always want to create a new Tet at the beginning
   * of the game.
   */
  newTet: boolean
  /**
   * The Tet that's falling and being controlled by the player.
   *
   * Defaults as null since we don't start off with any Tets the moment the game
   * gets intialized.
   */
  currTet: Tet | null
  /**
   * The Tet that's going to come into play after the currTet lands.
   *
   * Defaults as null since we don't start off with any Tets the moment the game
   * gets intialized.
   */
  nextTet: Tet | null
  /**
   * If true, we should update our landed array to be used in collision
   * detection.
   */
  updateLanded: boolean
  /** This is the array of all Tets that are in the game. */
  allTets: Tet[]
  /**
   * This is the array of all Tets that need to be removed before being drawn.
   */
  tetsToRemove: number[]
  /** This is the score that we're going to use to display. */
  score: number
  /**
   * This is the boolean we check to see if we should update our high score list
   * or not.
   */
  updateScore: boolean
  /** TODO: Add comment */
  loop: number
  /** TODO: Add comment */
  dropOnce: boolean

  // Private vars
  /**
   * This is the interval, in milliseconds, for which our currTet is going to
   * drop 1 block.
   */
  private dropInterval: number
  /**
   * The flag that indicates when the game is over. When true, we handle the
   * "game over" events.
   */
  private gameOver: boolean
  /**
   * This is the width that we set. This width can be adjusted and our game will
   * scale to it.
   */
  private canvasWidth: number
  /** This is the length of the side of each "block" on the game, in pixels. */
  private blockS: number
  /** This is the DOM element for which we are going to be drawing on. */
  private canvas: HTMLCanvasElement
  /**
   * This is the height of the panel which houses our score, nextTet, and
   * PAUSED/DEV text.
   */
  private panelHeight: number
  /**
   * This is the array of array of numbers which we are going to populate with
   * our allTets to be able to detect Tet collision.
   */
  private landed: number[][]
  /** TODO: Add comment */
  private paused: boolean
  /**
   * This is the name of the high score list DOM element for which we are going
   * to show our user their past high scores.
   */
  private highScoresListId: string

  private store: Store

  /**
   * Represents all of the functions which generate and control the game board.
   * We use the {@link Tet} class to manipulate our Tets.
   * @param canvasId This is the id of the canvas element within the document
   *     from which this Game class was created.
   * @param highScoresListId This is the id of the list for which we are going
   *     to list out the user's past high scores.
   * @param [devMode] This is the option to set the game to be initially in
   *     Developer's Mode.
   */
  constructor(canvasId: string, highScoresListId: string, devModeOn = false) {
    // Force instantiation
    if (!(this instanceof Game)) {
      return new Game(canvasId, highScoresListId, devModeOn)
    }

    // TODO: Add ability to pass in {options}
    this.devModeOn = devModeOn
    this.newTet = true
    this.currTet = null
    this.nextTet = null
    this.updateLanded = true
    this.allTets = []
    this.tetsToRemove = []
    this.score = 0
    this.updateScore = true

    // Private vars
    this.dropInterval = 750 // 750
    this.gameOver = false
    this.canvasWidth = 200

    // Assume block width and height will always be the same:
    this.blockS = this.canvasWidth / 10

    this.canvas = document.getElementById(canvasId) as HTMLCanvasElement
    this.canvas.width = this.canvasWidth
    this.canvas.height = 2 * this.canvasWidth

    this.panelHeight =
      Math.round((2 - Game.BOARD_ROW_NUM / Game.BOARD_COL_NUM) *
        this.canvasWidth)

    this.landed = []
    this.paused = true
    this.highScoresListId = highScoresListId

    this.store = new Store({
      configName: 'config',
      defaults: {
        highScores: [this.score, 0, 0, 0, 0, 0, 0, 0, 0, 0]
      }
    })

    // Init functions
    this.displayHighScores()
    this.createTet()
    this.handleEvents()
  }

  /**
   * This method creates 3 event listeners (2 for the window and 1 for the
   * document). The 2 events for the window are onblur and onfocus. These will
   * pause the game when you leave the game window and resume it when you come
   * back. The event for the document listens for onkeydown events. These
   * basically allow the user to interact with the game.
   */
  handleEvents() {
    const that = this
    // Pause if we lose focus of the game. Resume once we get focus back. We
    // don't need the Page Visibility API because we don't have a resource
    // intensive game while it's idle
    let pausedBeforeBlur = true
    window.onblur = () => {
      if (that.gameOver === false) {
        pausedBeforeBlur = that.paused
        clearInterval(that.loop)
        that.paused = true
        that.draw()
      }
    }
    window.onfocus = () => {
      this.canvas.focus()
      this.canvas.blur()
      if (!pausedBeforeBlur && that.gameOver === false) {
        if (!that.gameOver) {
          that.tetDownLoop()
        }
        that.paused = false
        that.draw()
      }
    }

    // Handle key events
    // For keycodes: http://www.javascripter.net/faq/keycodes.htm
    document.onkeydown = (e) => {
      switch (e.keyCode) {
        case 32: // space to move living Tet all the way down
          if (that.canTetMove() === true) {
            while (!that.newTet && that.currTet) {
              that.currTet.moveDown()
            }
            that.draw()
            that.tetDownLoop()
          }
          break
        case 38: // up arrow to rotate Tet clockwise
          if (that.canTetMove() === true && that.currTet) {
            that.currTet.rotate()
            that.draw()
          }
          break
        case 37: // left arrow to move Tet left
          if (that.canTetMove() === true && that.currTet) {
            that.currTet.moveLeft()
            that.draw()
          }
          break
        case 39: // right arrow to move Tet right
          if (that.canTetMove() === true && that.currTet) {
            that.currTet.moveRight()
            that.draw()
          }
          break
        case 40: // down arrow to move Tet down
          if (that.canTetMove() === true && that.currTet) {
            let skip = false
            if (that.newTet) skip = true
            if (!skip) clearInterval(that.loop)
            that.currTet.moveDown()
            that.draw()
            if (!skip && !that.paused) that.tetDownLoop()
          }
          break
        case 80: case 83: // p for pause, s for stop (they do same thing)
          if (that.gameOver === false) {
            if (!that.paused) {
              clearInterval(that.loop)
              that.paused = true
              that.draw()
            } else {
              if (that.gameOver === false) {
                that.tetDownLoop()
                that.dropOnce = false
              }
              that.paused = false
              that.draw()
            }
          }
          break
        case 82: // r for reset
          that.allTets = []
          clearInterval(that.loop)
          that.currTet = null
          that.gameOver = false
          that.newTet = true
          that.nextTet = null
          that.paused = true
          that.score = 0
          that.updateScore = true
          that.createTet()
          break
        // Developer's Controls
        case 35: // end key to move Tet up
          if (that.devModeOn && that.currTet) {
            if (that.currTet.topLeft.row > 0) {
              that.currTet.topLeft.row--
            }
            that.draw()
          }
          break
        case 48: case 49: case 50: case 51: case 52: // test cases found in TestCase.js
        case 53: case 54: case 55: case 56: case 57: // number keys 0 to 9 (not numpad)
          if (that.devModeOn) {
            that.allTets = []
            that.gameOver = false
            that.score = 0
            that.updateScore = true
            that.testCase(e.keyCode - 48)
            that.createTet()
            that.tetDownLoop()
          }
          break
        case 71: // g for game over
          if (that.devModeOn) {
            that.gameOver = true
            clearInterval(that.loop)
            // that.score = 1939999955999999 // near max
            that.score = Math.random() * 100000
            that.updateScore = true
            that.draw()
          }
          break
        case 72: // h to reset high score to zero
          if (that.devModeOn) {
            that.setHighScores([0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
            that.displayHighScores()
            that.draw()
          }
          break
        case 192: // tilde key to toggle dev mode
          that.devModeOn = that.devModeOn
          that.draw()
          break
        default:
          console.log('unrecognized key: ' + e.keyCode)
      }
    }
  }

  /**
   * This method is exclusively used in the handleEvents method. We call it
   * every time we want to check if our Tet can be moved with the space bar and
   * up/right/left/down arrow keys.
   * @returns If the Tet can move, based on the conditions within the function,
   *     then return true.
   */
  canTetMove() {
    return ((this.newTet === false && this.paused === false) ||
      this.devModeOn === true) && this.gameOver === false
  }

  /**
   * This method is used to get a floating point number and separate it with
   * commas. We also round the number to the nearest integer.
   * @param number A non-comma separated number.
   * @returns A comma separated number.
   */
  commaSeparateNumber(num: number) {
    const numIn = Math.floor(num)
    let numOut = numIn.toString()

    if (numIn <= 99999999999999) {
      // from http://stackoverflow.com/a/12947816
      const get3ConsecutiveDigits = /(\d+)(\d{3})/
      while (get3ConsecutiveDigits.test(numOut)) {
        numOut = numOut.replace(get3ConsecutiveDigits, '$1' + ',' + '$2')
      }
    } else if (numIn > 999999999999999) {
      numOut = numIn.toExponential(10)
    }

    return numOut
  }

  /**
   * This method updates the high score list that is displayed on the web page.
   */
  displayHighScores() {
    const highScores = this.getHighScores()
    let html = ''
    const len = highScores.length
    for (let i = 0; i < len; i++) {
      html += '<li>' + this.commaSeparateNumber(highScores[i]) + '</li>'
    }
    // TODO: Figure out of this is the best implementation of this
    let elem: any = document.getElementById(this.highScoresListId) || false
    if (elem) elem.innerHTML = html
  }

  /** This method draws everything to the canvas. */
  draw() {
    // Keys, in order, reflect the HTML color code of Tets: I, J, L, O, S, T, Z
    const tetColor = ['#3cc', '#0af', '#f90', '#ee0', '#0c0', '#c0c', '#c00']

    const c = this.canvas.getContext('2d')

    // TODO: Figure out a more graceful way of doing this
    if (!c || !this.nextTet) return

    c.clearRect(0, 0, this.canvas.width, 2 * this.canvas.width) // clear canvas

    // Draw top panel
    // paused
    if (this.paused) {
      c.fillStyle = '#f00'
      c.font = '16px Arial'
      c.fillText('PAUSED', 5, 74)
    }
    // score
    c.fillStyle = '#000'
    c.font = '16px Arial'
    // 16 numbers max, or 14 with commas. If beyond, switch to scientific
    // notation:
    c.fillText('Score: ' + this.commaSeparateNumber(this.score), 4, 17)
    // next Tet
    c.font = '16px Arial'
    c.fillText('Next:', 35, 50)
    c.beginPath()
    c.moveTo(
      (this.nextTet.topLeft.col + this.nextTet.perim[0][0]) *
      this.blockS,
      (this.nextTet.topLeft.row + this.nextTet.perim[0][1]) *
      this.blockS + 37)
    const len = this.nextTet.perim.length
    for (let row = 1; row < len; row++) {
      c.lineTo(
        (this.nextTet.topLeft.col + this.nextTet.perim[row][0]) *
        this.blockS,
        (this.nextTet.topLeft.row + this.nextTet.perim[row][1]) *
        this.blockS + 37)
    }
    c.closePath()
    c.lineWidth = 2
    c.fillStyle = tetColor[this.nextTet.type]
    c.fill()
    c.strokeStyle = '#000'
    c.stroke()
    // separator line
    c.beginPath()
    c.moveTo(0, this.panelHeight)
    c.lineTo(this.canvasWidth, this.panelHeight)
    c.lineWidth = 2
    c.strokeStyle = '#eee'
    c.stroke()
    c.beginPath()
    c.moveTo(0, this.panelHeight)
    c.lineTo(4 * this.blockS - 3, this.panelHeight)
    c.lineTo(4 * this.blockS - 3, 2 * this.blockS - 6)
    c.lineTo(2 * 4 * this.blockS + 3, 2 * this.blockS - 6)
    c.lineTo(2 * 4 * this.blockS + 3, this.panelHeight)
    c.lineTo(this.canvasWidth, this.panelHeight)
    c.lineWidth = 2
    c.strokeStyle = '#000'
    c.stroke()
    // dev mode indicator
    if (this.devModeOn) {
      c.fillStyle = '#0a0'
      c.font = '15px Arial'
      c.fillText('DEV', 166, 74)
    }

    // Draw living Tet "shadow" at bottom and rotation
    if (!this.newTet) {
      // TODO: Figure out a more graceful way of doing this
      if (!this.currTet) {
        return
      }

      const tmpPotTopLeft = {
        row: this.currTet.topLeft.row + 1,
        col: this.currTet.topLeft.col
      }
      while (!this.currTet.doesTetCollideBot(tmpPotTopLeft)) {
        tmpPotTopLeft.row++
      }
      tmpPotTopLeft.row--
      c.beginPath()
      c.moveTo(
        (tmpPotTopLeft.col + this.currTet.perim[0][0]) *
        this.blockS,
        (tmpPotTopLeft.row + this.currTet.perim[0][1]) *
        this.blockS + this.panelHeight)
      const len = this.currTet.perim.length
      for (let row = 1; row < len; row++) {
        c.lineTo(
          (tmpPotTopLeft.col + this.currTet.perim[row][0]) *
          this.blockS,
          (tmpPotTopLeft.row + this.currTet.perim[row][1]) *
          this.blockS + this.panelHeight)
      }
      c.closePath()
      c.lineWidth = 2
      c.fillStyle = '#eee'
      c.fill()
      c.strokeStyle = '#ddd'
      c.stroke()

      // draw pivot shadow
      if (this.currTet.pivot > 0) {
        const potPerim = this.currTet.doesNotTetPivotCollide()
        if (potPerim !== false) {
          c.beginPath()
          c.moveTo(
            (this.currTet.topLeft.col + potPerim[0][0] + this.currTet.pivot) *
            this.blockS,
            (this.currTet.topLeft.row + potPerim[0][1]) *
            this.blockS + this.panelHeight)
          const len = this.currTet.perim.length
          for (let row = 1; row < len; row++) {
            c.lineTo(
              (this.currTet.topLeft.col + potPerim[row][0] +
                this.currTet.pivot) * this.blockS,
              (this.currTet.topLeft.row + potPerim[row][1]) *
              this.blockS + this.panelHeight)
          }
          c.closePath()
          c.lineWidth = 2
          c.globalAlpha = 0.5
          c.fillStyle = '#eee'
          c.fill()
          c.strokeStyle = '#ddd'
          c.stroke()
          c.globalAlpha = 1
        }
      }
    }

    // Draw all Tets
    const aTLen = this.allTets.length
    for (let tet = 0; tet < aTLen; tet++) {
      const currTet = this.allTets[tet]
      c.beginPath()
      c.moveTo(
        (currTet.topLeft.col + currTet.perim[0][0]) * this.blockS,
        (currTet.topLeft.row + currTet.perim[0][1]) * this.blockS +
        this.panelHeight)
      const len = currTet.perim.length
      for (let row = 1; row < len; row++) {
        c.lineTo(
          (currTet.topLeft.col + currTet.perim[row][0]) * this.blockS,
          (currTet.topLeft.row + currTet.perim[row][1]) * this.blockS +
          this.panelHeight)
      }
      c.closePath()
      c.lineWidth = 2
      c.fillStyle = tetColor[currTet.type]
      c.fill()
      c.strokeStyle = '#000'
      c.stroke()
    }

    // Draw Game Over text if game is over
    if (this.gameOver) {
      // gray tint
      c.globalAlpha = 0.8
      c.fillStyle = '#333'
      c.fillRect(0, 0, this.canvas.width, 2 * this.canvas.width)
      c.globalAlpha = 1
      // game over text
      c.fillStyle = '#f00'
      c.font = 'bold 32px Arial'
      c.fillText('GAME OVER', 3, 180)
      c.strokeStyle = '#000'
      c.lineWidth = 1
      c.strokeText('GAME OVER', 3, 180)
      // your score
      c.fillStyle = '#fff'
      c.font = 'bold 18px Arial'
      c.fillText('Your Score:', 5, 220)
      c.fillStyle = '#f00'
      c.font = 'bold 19px Arial'
      c.fillText(this.commaSeparateNumber(this.score), 14, 240)
      c.globalAlpha = 0.5
      c.strokeStyle = '#000'
      c.lineWidth = 1
      c.font = 'bold 18px Arial'
      c.strokeText('Your Score:', 5, 220)
      c.font = 'bold 19px Arial'
      c.strokeText(this.commaSeparateNumber(this.score), 14, 240)
      c.globalAlpha = 1
      // personal highest score
      const highscores = this.checkHighScore()
      c.fillStyle = '#fff'
      c.font = 'bold 17px Arial'
      c.fillText('Personal Highest Score:', 5, 270)
      c.fillStyle = '#f00'
      c.font = 'bold 19px Arial'
      c.fillText(this.commaSeparateNumber(highscores[0]), 14, 290)
      c.globalAlpha = 0.3
      c.strokeStyle = '#000'
      c.lineWidth = 1
      c.font = 'bold 17px Arial'
      c.strokeText('Personal Highest Score:', 5, 270)
      c.font = 'bold 19px Arial'
      c.strokeText(this.commaSeparateNumber(highscores[0]), 14, 290)
      c.globalAlpha = 1
      this.displayHighScores()
    }
  }

  /**
   * This method creates Tets. This also causes the Game Over screen to appear
   * when we cannot create a new Tet.
   */
  createTet() {
    // Make sure first Tet is not an S or Z
    if (this.nextTet === null) {
      let t: number = Math.floor(Math.random() * 7)
      // TODO: Instead of doing this, keep randomly generating a number until
      // it's not a 4 or 6. This way there isn't a higher likelihood of starting
      // off with a 3 or 5 (should be as fairly random as possible) - plus there
      // isn't a performance issue to worry about since this gets generated
      // before the game even starts.
      if (t === 4 || t === 6) {
        t--
      }
      this.nextTet = new Tet(this, t)
    }

    // Build first Tet and next Tet
    if (this.newTet) {
      this.currTet = this.nextTet
      this.nextTet = new Tet(this)
    }
    this.newTet = false

    // TODO: Figure out how to make this check unnecessary since ideally this
    // would never be null.
    if (!this.currTet) {
      this.currTet = this.nextTet
    }

    // Display Game Over
    if (this.currTet.doesTetCollideBot(this.currTet.topLeft)) {
      this.nextTet = this.currTet
      this.gameOver = true
      this.newTet = true
      clearInterval(this.loop)
      return
    } else {
      this.allTets.push(this.currTet)
    }

    this.draw()
  }

  /**
   * This method creates a setInterval loop which moves our currTet down at
   * each interval.
   */
  tetDownLoop() {
    // safe guard to prevent multiple loops from spawning before clearing it out
    // first
    clearInterval(this.loop)

    const that = this
    this.loop = window.setInterval(() => {
      if (that.dropOnce && that.newTet) clearInterval(that.loop)
      if (that.newTet) that.createTet()
      else if (!that.paused && that.currTet) that.currTet.moveDown()
      that.draw()
    }, that.dropInterval)
  }

  /**
   * This method generates a landed array from allTets to be used to check for
   * Tet/fragment collisions.
   * @param tet This parameter basically excludes the given Tet from allTets
   *     which are used to generate the landed array.
   */
  getLanded(tet?: Tet) {
    if (tet !== undefined) this.updateLanded = true
    if (this.updateLanded) {
      for (let i = 0; i < Game.BOARD_ROW_NUM; i++) {
        this.landed[i] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
      }
      const aT = this.allTets
      const len = aT.length
      for (let i = 0; i < len; i++) {
        if (aT[i] === this.currTet || aT[i] === tet) continue
        const rLen = aT[i].shape.length
        for (let row = 0; row < rLen; row++) {
          const cLen = aT[i].shape[row].length
          for (let col = 0; col < cLen; col++) {
            if (aT[i].shape[row][col] !== 0) {
              this.landed[row + aT[i].topLeft.row][col + aT[i].topLeft.col] = 1
            }
          }
        }
      }
      this.updateLanded = false
    }

    return this.landed
  }

  /**
   * This method inserts all zeros into the rows of the shape array if they are
   * going to be removed. Once we do this, we call the updateLanded method.
   * @param fullRows This is the list of all rows that are to be removed from
   *     the Tet shapes.
   */
  alterShapes(fullRows: number[]) {
    const firstRow = fullRows[0]
    const lastRow = fullRows[fullRows.length - 1]
    const len = this.allTets.length
    for (let tet = 0; tet < len; tet++) {
      if (this.allTets[tet].topLeft.row <= firstRow - 4 ||
        this.allTets[tet].topLeft.row > lastRow) {
        continue
      }
      this.allTets[tet].alterShape(fullRows)
    }
    // this.tetsToRemove.sort(function(a,b){ return a - b }) // ensures indices
    // are in numeric order
    const len2 = this.tetsToRemove.length
    for (let i = 0; i < len2; i++) {
      this.allTets.splice(this.tetsToRemove[i] - i, 1)
    }
    this.tetsToRemove = []
    this.updateLanded = true
  }

  /**
   * This method gets the user's high scores from their cookie.
   * @returns This is the list of the high scores of the user.
   */
  getHighScores() {
    return this.store.get('highScores')
  }

  /**
   * This method saves the user's high scores into the cookie.
   * @param v This is the list of the high scores we're going to save in the
   *     cookie.
   */
  setHighScores(v: number[]) {
    // console.log('setting high scores', v) // debug
    this.store.set('highScores', v)
  }

  /**
   * This method basically adjusts the user's high scores if they made a higher
   * score than before.
   * @returns This is the list of the high scores of the user.
   */
  checkHighScore() {
    const highScores = this.getHighScores()
    if (this.updateScore === true) {
      const hsLen = highScores.length
      for (let i = 0; i < hsLen; i++) {
        if (this.score > highScores[i]) {
          highScores.splice(i, 0, this.score.toFixed(2))
          break
        }
      }
      if (highScores.length > hsLen) highScores.pop()
      this.setHighScores(highScores)
      this.updateScore = false
    }

    return highScores
  }

  // TODO: This is from TestCase.js
  testCase(n: number) {
    console.warn('Test cases not enabled yet.' + n)
  }
}