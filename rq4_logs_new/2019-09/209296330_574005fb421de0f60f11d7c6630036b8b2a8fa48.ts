import utils from "../../node_modules/decentraland-ecs-utils/index"

export class Point extends Entity {
    point: number
    position: Vector3

    constructor(point: number, position: Vector3, rotation: Quaternion) {
        super()
        engine.addEntity(this)
        this.addComponent(new Transform({ position: Vector3.Zero(), rotation }))
        this.addComponent(new TextShape(`+${point.toString()}`));
        this.addComponentOrReplace(new utils.MoveTransformComponent(position, position.add(POINT_POSITION), DISPLAY_TIME, () => engine.removeEntity(this)))
    }
}

const POINT_POSITION = new Vector3(0, 0.5, 0)
const DISPLAY_TIME = 0.5