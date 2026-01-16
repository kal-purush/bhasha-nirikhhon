import Fractral from './Fractral'
import * as _three from 'three'
import Scene from '../scene/Scene'

export default class Mengersponge extends Fractral {
  private x: number
  private y: number
  private z: number
  private width: number
  private state: number
  private object: _three.Object3D

  public constructor(
    x: number,
    y: number,
    z: number,
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
    this.z = z
    this.width = width
    this.state = state
    this.object = new _three.Object3D()
  }

  public init(): void {
    const scene = this.getMainScene.getScene
    const camera = this.getMainScene.getCamera
    // Add cube to Scene
    this.calcMengersponge(scene, this.x, this.y, this.z, this.width, this.state, this.getLevel, this.getColor)
    // Add light to scene
    const spotLight = new _three.SpotLight(0xffffff, 0.9)
    const ambLight = new _three.AmbientLight(0xffffff, 0.3)
    spotLight.position.set(0, 100, 50)
    spotLight.castShadow = true
    scene.add(spotLight)
    scene.add(ambLight)
  }

  private calcMengersponge(
    scene: _three.Scene,
    x: number,
    y: number,
    z: number,
    width: number,
    state: number,
    level: number,
    color: string,
  ): void {
    if (state === level) {
      scene.add(this.calcCube(x, y, z, width, color))
    } else {
      const ns = width / 3
      for (let i = 0; i < 3; i++) {
        for (let j = 0; j < 3; j++) {
          for (let k = 0; k < 3; k++) {
            if ((i !== 1 && j !== 1) || (k !== 1 && j !== 1) || (i !== 1 && k !== 1)) {
              this.calcMengersponge(scene, x + i * ns, y + j * ns, z + k * ns, ns, state + 1, level, color)
            }
          }
        }
      }
    }
  }

  private calcCube(x: number, y: number, z: number, width: number, color: string): _three.Mesh {
    const CUBE_MATERIAL = new _three.MeshPhongMaterial({ color: color, flatShading: true, shininess: 2 })
    const geometry = new _three.BoxBufferGeometry(width, width, width)
    const cube = new _three.Mesh(geometry, CUBE_MATERIAL)
    cube.position.set(x, y, z)
    cube.receiveShadow = true
    return cube
  }
}