const print = console.log;

enum Player {
  User, 
  Computer
}

export default class Game {
  static player: Player = Player.User;
  board: Board = new Board()

  constructor() {
    print("Initialising game")
  }
}

class Board {
  // board
  rows = 6
  columns = 7

  slots: Slot[] = []
  
  constructor() {
    this.setup()

    this.printBoard()

    let chosenSlot = this.chooseSlot(4, 5)
    if (chosenSlot) {
      // evaluate for connect four
      // switch player
    }

  }

  // life cycle
  setup() {
    for (let s = 0; s < this.rows; s++) {
      for (let c = 0; c < this.columns; c++) {
        let newSlot = new Slot(s, c)
        this.slots.push(newSlot)
      }
    }
  }

  clear() {
    this.slots = []
  }

  chooseSlot(rowIndex: number, columnIndex: number): Slot | null {
    let chosenSlot = this.slots.find(item => item.rowIndex == rowIndex && item.colIndex == columnIndex )

    if (chosenSlot) {
      print(`Chose slot at ${chosenSlot.desc}`)
      chosenSlot.filledBy = Player.Computer
      this.printBoard()

      return chosenSlot
    }

    return null
  }

  checkForConnectFour(arr: Slot[], isRow: Boolean): Boolean {
    let count = 0
    let filled = arr
      .filter(slot => slot.filledBy == Game.player)
      .map(item => {
        return new SlotForEvaluation(isRow ? item.rowIndex : item.colIndex)
      })
      .sort((a, b) => a.index - b.index) // double check it's sorted
    
    if (filled.length < 4) {
      return false
    }

    for (var i = 1; i < filled.length; i++) {
      if (filled[i].index - filled[i-1].index != 1 ) {
        return false
      } 
      count ++

      if (count >= 4) {
        return true
      }
    }

    return false 
  }

  // evaluate relevant slots
  columnFromSlot(newSlot: Slot): Slot[] {
    return this.slots.filter(slot => slot.colIndex === newSlot.colIndex)
  } 

  rowFromSlot(newSlot: Slot): Slot[] {
    return this.slots.filter(slot => slot.rowIndex === newSlot.rowIndex)
  } 

  leftDownDiagonalFromSlot(newSlot: Slot): Slot[] {
    return this.slots.filter(slot => {
      let slotSum = slot.rowIndex + slot.colIndex
      let newSlotSum = newSlot.rowIndex + newSlot.colIndex
      
      return newSlotSum === slotSum
    })
  }
  rightDownDiagonalFromSlot(newSlot: Slot): Slot[] {
    return this.slots.filter(slot => {
      let slotSum = slot.rowIndex - slot.colIndex
      let newSlotSum = newSlot.rowIndex - newSlot.colIndex
      
      return newSlotSum === slotSum
    })
  }

  // printing
  printBoard() {
    for (let s = 0; s < this.rows; s++) {
      let rowString = ""
      let row = this.slots.filter(slot => slot.rowIndex === s)
      row.forEach(item => rowString += item.desc)

      print(rowString)
    }
  }

  printRowsForEvaluation(chosenSlot: Slot) {
    // get columns
      print("Row: ")
      print(this.rowFromSlot(chosenSlot).map(item => item.desc).join(""))

      print("Column:")
      this.columnFromSlot(chosenSlot).forEach(item => print(item.desc))

      print("Right diagonal:")
      this.rightDownDiagonalFromSlot(chosenSlot).forEach(item => print(item.desc))
      
      print("Left diagonal")
      this.leftDownDiagonalFromSlot(chosenSlot).forEach(item => print(item.desc))
  }

}


enum SlotState {
  Empty, Filled
}

class Slot {
  filledBy: Player | null = null
  rowIndex: number
  colIndex: number

  colLetter: string

  constructor(slotIndex: number, colIndex: number) {
    this.rowIndex = slotIndex
    this.colIndex = colIndex

    // for convenience sake
    this.colLetter = String.fromCharCode(97 + this.colIndex);
  }

    get desc(): string {
      let empty = `${this.rowIndex}${this.colLetter}`
      let filled = "  "
      return `[${this,this.filledBy === null ? empty : filled }]`;
    } 
}

// TODO: Consider if necessary
class SlotForEvaluation {
  index: number

  constructor(index: number) {
    this.index = index
  }
}