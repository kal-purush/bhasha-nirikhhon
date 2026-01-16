import utils from "../../node_modules/decentraland-ecs-utils/index"
import { ActionsSequenceSystem } from "../../node_modules/decentraland-ecs-utils/actionsSequenceSystem/actionsSequenceSystem";
import { RedPhoton, GreenPhoton } from '../particles/index'
import { Literal } from '../sequences/index'
import { MoveAction } from '../actions/move_action'

// const redBase = new Vector3(40, 12, 7.5)
// let redPhoton: RedPhoton[] = RedPhoton.buildInitArray(15)

// const greenBase = new Vector3(40, 12, 9)
// let greenPhoton: GreenPhoton[] = GreenPhoton.buildInitArray(15)


export class RiseAction implements ActionsSequenceSystem.IAction {
  hasFinished: boolean = false;
  entity: Entity
  position: Vector3
  basePosition: Vector3

  redBase: Vector3
  greenBase: Vector3
  redPhoton: RedPhoton[]
  greenPhoton: GreenPhoton[]

  constructor(entity: Entity, position: Vector3, basePosition: Vector3) {
    this.entity = entity
    this.position = position
    this.basePosition = basePosition
    this.redBase = new Vector3(40, 12, 7.5)
    this.greenBase = new Vector3(40, 12, 9)
    this.redPhoton = RedPhoton.buildInitArray(15)
    this.greenPhoton = GreenPhoton.buildInitArray(15)
  }

  //Method when action starts
  onStart(): void {
    const transform = this.entity.getComponent(Transform)
    const toPosition = new Vector3(this.basePosition.x + this.position.x, this.basePosition.y + this.position.y, this.basePosition.z + this.position.z)
    this.entity.addComponentOrReplace(new utils.MoveTransformComponent(transform.position, toPosition, 5,
      () => {
        this.hasFinished = true
      }))
  }
  //Method to run on every frame
  update(dt: number): void {
  }
  //Method to run at the end
  onFinish(): void {
    log('onFinish')
    const redTrigger = new RedPhoton(this.redBase)
    log(redTrigger)
    redTrigger.addComponent(
      new OnClick(() => {
        log('click red photon')
        log(new Entity())

        const sequence = new utils.ActionsSequenceSystem.SequenceBuilder()
          .while(() => null)
          .then(new MoveAction(this.redPhoton[0], new Vector3(1, 0, 1), this.basePosition))
          .endWhile()

          //////////////////////////////////////////////
          // A
          //////////////////////////////////////////////

          .then(new MoveAction(this.redPhoton[0], new Vector3(2, 1, 1), this.basePosition))
          .then(new MoveAction(this.redPhoton[1], new Vector3(2, 2, 1.5), this.basePosition))
          .then(new MoveAction(this.redPhoton[2], new Vector3(2, 3, 2), this.basePosition))
          .then(new MoveAction(this.redPhoton[3], new Vector3(2, 4, 2.5), this.basePosition))
          .then(new MoveAction(this.redPhoton[4], new Vector3(2, 3, 2.5), this.basePosition))
          .then(new MoveAction(this.redPhoton[5], new Vector3(2, 5, 3), this.basePosition))
          .then(new MoveAction(this.redPhoton[6], new Vector3(2, 4, 3.5), this.basePosition))
          .then(new MoveAction(this.redPhoton[7], new Vector3(2, 3, 3.5), this.basePosition))
          .then(new MoveAction(this.redPhoton[8], new Vector3(2, 3, 4), this.basePosition))
          .then(new MoveAction(this.redPhoton[9], new Vector3(2, 2, 4.5), this.basePosition))
          .then(new MoveAction(this.redPhoton[10], new Vector3(2, 1, 5), this.basePosition))
          .then(new MoveAction(this.redPhoton[11], new Vector3(0, 0, 0), this.basePosition))
          .then(new MoveAction(this.redPhoton[12], new Vector3(0, 0, 0), this.basePosition))
          .then(new MoveAction(this.redPhoton[13], new Vector3(0, 0, 0), this.basePosition))
          .then(new MoveAction(this.redPhoton[14], new Vector3(0, 0, 0), this.basePosition))
        engine.addSystem(new utils.ActionsSequenceSystem(sequence))
        // TODO: [error] Why didn't I create an instance
        // const literal = new Literal(new Vector3(40, 12, 7.5))
        // log(literal)



        //     const sequence = literal.buildDECEN(this.redPhoton)
        //     engine.addSystem(new utils.ActionsSequenceSystem(sequence))
      })
    )
    // const greenTrigger = new GreenPhoton(this.greenBase)
    // greenTrigger.addComponent(
    //   new OnClick(() => {
    //     const literal = new Literal(this.greenBase)
    //     const sequence = literal.buildAFrom(this.greenPhoton)
    //     engine.addSystem(new utils.ActionsSequenceSystem(sequence))
    //   })
    // )
  }
}