/// <reference path="../config/index.ts" />
import utils from "../../node_modules/decentraland-ecs-utils/index"
import { ActionsSequenceSystem } from "../../node_modules/decentraland-ecs-utils/actionsSequenceSystem/actionsSequenceSystem";

export class FollowAction implements ActionsSequenceSystem.IAction {
  hasFinished: boolean = false;
  entity: Entity
  position: Vector3

  constructor(entity: Entity) {
    this.entity = entity
    this.position = entity.getComponent(Transform).position
  }

  //Method when action starts
  onStart(): void {
    let x = this.position.x
    let y = this.position.y
    let z = this.position.z
    let path = []
    path[0] = new Vector3(x+5, y, z)
    path[1] = new Vector3(x+5, y, z+5)
    path[2] = new Vector3(x+5, y+5, z+5)
    path[3] = new Vector3(x, y+10, z+10)
    path[4] = new Vector3(x-10, y+5, z)
    path[5] = new Vector3(x-10, y-10, z+5)
    path[6] = new Vector3(x-15, y+15, z+5)
    path[7] = new Vector3(x-20, y+10, z-10)
    path[8] = new Vector3(x-15, y+5, z+10)
    path[9] = new Vector3(x, y, z)
    this.entity.addComponent(new utils.FollowPathComponent(path, 30))
  }
  //Method to run on every frame
  update(dt: number): void {
  }
  //Method to run at the end
  onFinish(): void {
  }
}