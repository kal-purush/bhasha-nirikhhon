/// <reference path="../config/index.ts" />
export class HintExplain extends Entity {

  constructor() {
    super()
    engine.addEntity(this)
    this.addComponent(new Transform({
      position: new Vector3(-2, 9, 5),
      scale: new Vector3(0.1, 0.11, 0.1),
      rotation: Quaternion.Euler(0, 180, 0)
    }))
    this.addComponent(new TextShape(
      'DROP FROM THE GAME STAGE \n\n ON THE CORRIDOR \n\n THEN YOU CAN FIND \n\n A GREEN PHOTON'
    ))
  }
}