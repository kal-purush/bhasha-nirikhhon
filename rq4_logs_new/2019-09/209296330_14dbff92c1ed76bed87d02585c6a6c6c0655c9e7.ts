/// <reference path="../config/index.ts" />
import utils from "../../node_modules/decentraland-ecs-utils/index"
import { ActionsSequenceSystem } from "../../node_modules/decentraland-ecs-utils/actionsSequenceSystem/actionsSequenceSystem";
import { BluePhoton } from "../particles/index";
import { ScoreBoard } from "../components/index";

export class BubbleAction implements ActionsSequenceSystem.IAction {
  hasFinished: boolean = false
  scoreBoard: ScoreBoard

  constructor(scoreBoard: ScoreBoard) {
    this.scoreBoard = scoreBoard
  }

  //Method when action starts
  onStart(): void {
    BluePhoton.buildBubble(new Vector3(35, 20, 7.5), this.scoreBoard)
    BluePhoton.buildBubble(new Vector3(35, 20, 6.5), this.scoreBoard)
    BluePhoton.buildBubble(new Vector3(35, 20, 5.5), this.scoreBoard)
  }
  //Method to run on every frame
  update(dt: number): void {
  }
  //Method to run at the end
  onFinish(): void {
  }
}