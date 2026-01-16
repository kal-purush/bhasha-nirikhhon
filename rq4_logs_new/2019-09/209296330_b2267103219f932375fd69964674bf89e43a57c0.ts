/// <reference path="../config/index.ts" />
import utils from "../../node_modules/decentraland-ecs-utils/index"
import { ActionsSequenceSystem } from "../../node_modules/decentraland-ecs-utils/actionsSequenceSystem/actionsSequenceSystem";
import { BluePhoton, RedPhoton, GreenPhoton } from '../particles/index'
import { Literal } from '../sequences/index'
import { MoveAction } from '../actions/move_action'

export class CountdownAction implements ActionsSequenceSystem.IAction {
  hasFinished: boolean = false;

  constructor() {

  }

  //Method when action starts
  onStart(): void {
    const countdownText = new Entity()
    engine.addEntity(countdownText)
    let time = 3
    countdownText.addComponent(new Transform({ position: new Vector3(40, 18, 7.5), scale: new Vector3(2,2,2), rotation: Quaternion.Euler(0, 90, 0) }))
    countdownText.addComponent(new TextShape(time.toString()))
    countdownText.addComponent(new utils.Interval(1000, (): void => {
      time--
      if (time < 0) {
        this.hasFinished = true
        countdownText.removeComponent(utils.Interval)
        countdownText.removeComponent(TextShape)
      }
      countdownText.getComponent(TextShape).value = time.toString()
    }))
  }
  //Method to run on every frame
  update(dt: number): void {
  }
  //Method to run at the end
  onFinish(): void {
  }
}