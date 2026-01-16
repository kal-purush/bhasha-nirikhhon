
import utils from "../../node_modules/decentraland-ecs-utils/index"
import { ActionsSequenceSystem } from "../../node_modules/decentraland-ecs-utils/actionsSequenceSystem/actionsSequenceSystem";

//Use IAction to define action for scaling
export class ScaleAction implements ActionsSequenceSystem.IAction {
    hasFinished: boolean = false;
    entity: Entity
    scale: Vector3

    constructor(entity: Entity, scale: Vector3) {
        this.entity = entity
        this.scale = scale
    }

    //Method when action starts
    onStart(): void {
        const transform = this.entity.getComponent(Transform)
        this.hasFinished = false

        this.entity.addComponentOrReplace(new utils.ScaleTransformComponent(transform.scale, this.scale, 1,
            () => {
                this.hasFinished = true
            }, utils.InterpolationType.EASEINQUAD))
    }
    //Method to run on every frame
    update(dt: number): void {
    }
    //Method to run at the end
    onFinish(): void {
    }
}