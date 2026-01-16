import Fractal from './Fractal'
import * as _three from 'three'
import Scene from '../scene/Scene'

export default class SierpinskiCarpet extends Fractal {
  private x: number
  private y: number
  private width: number
  private state: number

  public constructor(
    x: number,
    y: number,
    width: number,
    state: number,
    level: number,
    scene: Scene,
    name: string,
    color: string,
  ) {
    super(scene, name, color, level)
    this.x = x
    this.y = y
    this.width = width
    this.state = state
  }

  public init(): void {
    const scene = this.getMainScene.getScene
    // Add rect to Scene
    this.calcSierpinskiCarpet(scene, this.x, this.y, this.width, this.state, this.getLevel, this.getColor)
    // Add light to scene
    const spotLight = new _three.SpotLight(0xffffff, 0.9)
    const ambLight = new _three.AmbientLight(0xffffff, 0.3)
    spotLight.position.set(0, 100, 50)
    spotLight.castShadow = true
    scene.add(spotLight)
    scene.add(ambLight)
  }

  private calcSierpinskiCarpet(
    scene: _three.Scene,
    x: number,
    y: number,
    width: number,
    state: number,
    level: number,
    color: string,
  ): void {
    if (state === level) {
      scene.add(this.calcrect(x, y, width, color))
    } else {
      const ns = width / 3
      for (let i = 0; i < 3; i++) {
        for (let j = 0; j < 3; j++) {
          for (let k = 0; k < 3; k++) {
            if ((i !== 1 && j !== 1) || (k !== 1 && j !== 1) || (i !== 1 && k !== 1)) {
              this.calcSierpinskiCarpet(scene, x + i * ns, y + j * ns, ns, state + 1, level, color)
            }
          }
        }
      }
    }
  }

  private calcrect(x: number, y: number, width: number, color: string): _three.Mesh {
    const material = new _three.MeshBasicMaterial({ color: color, side: _three.DoubleSide })
    const rect = new _three.PlaneGeometry(width, width)
    const plane = new _three.Mesh(rect, material)
    plane.position.set(x, y, 0)
    return plane
  }
}