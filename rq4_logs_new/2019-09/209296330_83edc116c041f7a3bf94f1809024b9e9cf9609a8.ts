import { BluePhoton, RedPhoton, GreenPhoton } from '../particles/index'
/// <reference path="../config/index.ts" />

export class GameExplain extends Entity {

  constructor() {
    super()
    engine.addEntity(this)
    this.addComponent(new Transform({
      position: new Vector3(21, 12, 9.5),
      scale: new Vector3(0.5, 0.5, 0.5)
    }))
    this.addComponent(new TextShape(
      'A Shooting Game starts when you get on the back stage ->'
    ))
  }

  // public static setUpExplains(): Entity[] {
  //   const explain = new GameExplain()

  //   const bluePhoton = new BluePhoton(new Vector3(26, 12, 5.5))
  //   const explainBlue = new Entity()
  //   explainBlue.addComponent(new Transform({
  //     position: new Vector3(25, 12, 5.5),
  //     rotation: Quaternion.Euler(0, 180, 0),
  //     scale: new Vector3(0.5, 0.5, 0.5)
  //   }))
  //   explainBlue.addComponent(new TextShape(
  //     ':1pt'
  //   ))
  //   engine.addEntity(explainBlue)
  //   const redPhoton = new RedPhoton(new Vector3(23, 12, 5.5))
  //   const explainRed = new Entity()
  //   explainRed.addComponent(new Transform({
  //     position: new Vector3(22, 12, 5.5),
  //     rotation: Quaternion.Euler(0, 180, 0),
  //     scale: new Vector3(0.5, 0.5, 0.5)
  //   }))
  //   explainRed.addComponent(new TextShape(
  //     ':Xpt'
  //   ))
  //   engine.addEntity(explainRed)
  //   const greenPhoton = new GreenPhoton(new Vector3(20, 12, 5.5))
  //   const explainGreen = new Entity()
  //   explainGreen.addComponent(new Transform({
  //     position: new Vector3(18, 12, 5.5),
  //     rotation: Quaternion.Euler(0, 180, 0),
  //     scale: new Vector3(0.5, 0.5, 0.5)
  //   }))
  //   explainGreen.addComponent(new TextShape(
  //     '(Hidden):20pt'
  //   ))
  //   engine.addEntity(explainGreen)
    
  //   const explanations = [explain, bluePhoton, explainBlue, redPhoton, explainRed, greenPhoton, explainGreen]
  //   log('xxx')
  //   log(explanations)
  //   return explanations
  // }

  // static setUp() {
  //   return new GameExplain()
  // }
}