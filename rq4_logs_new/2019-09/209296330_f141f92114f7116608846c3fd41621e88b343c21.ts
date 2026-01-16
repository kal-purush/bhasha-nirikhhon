import { addScore, saveRecord, getRecords } from '../states/store'

/// <reference path="../config/index.ts" />
export class RecordBoardText extends Entity {

  constructor() {
    super()
    engine.addEntity(this)
    this.addComponent(new Transform({ 
      position: Position.recordBoardText, 
      rotation: Rotate.recordBoardText, 
      scale: Scale.recordBoardText }))
    this.addComponent(new TextShape())
  }

  saveRecord(score: number): void {
    // TODO: if the size of the array exceeds 3, then removing
    saveRecord(score)
    this.refresh()
    log('SAVE RECORD')
  }

  refresh(): void {
    const recordStr = getRecords()
      .sort((a, b) => a - b)
      .map((r, i) => {
        switch (i) {
          case 0:
            return `FIRST : ${r} point \n`
          case 1:
            return `SECOND : ${r} point \n`
          case 2:
            return `THIRD : ${r} point`
          default:
            log('error')
            break
        }
      }).join(' ')
    
    this.getComponent(TextShape).value = recordStr
  }
}