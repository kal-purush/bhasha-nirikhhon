import utils from "../../node_modules/decentraland-ecs-utils/index"
import { ActionsSequenceSystem } from "../../node_modules/decentraland-ecs-utils/actionsSequenceSystem/actionsSequenceSystem";

const DEFAULT_FADE_OUT_SCALE = new Vector3(0, 0, 0)
const DEFAULT_FADE_IN_SCALE = new Vector3(1, 1, 1)

export const FadeOutAction = (
    entity: Entity, duration: number, positionDiff = Vector3.Zero(), toScale = DEFAULT_FADE_OUT_SCALE, initialScale?: Vector3, initialPosition?: Vector3
) => new FadeAction(entity, toScale, initialScale, duration, positionDiff, initialPosition)

export const FadeInAction = (
    entity: Entity, duration: number, positionDiff = Vector3.Zero(), initialPosition?: Vector3, toScale = DEFAULT_FADE_IN_SCALE
) => new FadeAction(entity, toScale, Vector3.Zero(), duration, positionDiff, initialPosition)

export class FadeAction implements ActionsSequenceSystem.IAction {
    hasFinished: boolean = false;
    toScale: Vector3
    initialScale: Vector3
    entity: Entity
    positionDiff: Vector3
    duration: number
    initialPositon: Vector3

    constructor(entity: Entity, toScale: Vector3, initialScale: Vector3, duration: number, positionDiff = Vector3.Zero(), initialPosition?: Vector3) {
        this.entity = entity
        this.toScale = toScale
        this.initialScale = initialScale
        this.positionDiff = positionDiff
        this.duration = duration
        this.initialPositon = initialPosition
    }

    //Method when action starts
    onStart(): void {
        const transform = this.entity.getComponent(Transform)
        const currentScale = this.initialScale || transform.scale
        this.entity.addComponentOrReplace(new utils.ScaleTransformComponent(currentScale, this.toScale, this.duration,
            () => {
                this.hasFinished = true
            }, utils.InterpolationType.EASEOUTQUAD))
        const currentPosition = this.initialPositon || transform.position
        const toPosition = currentPosition.add(this.positionDiff)
        this.entity.addComponentOrReplace(new utils.MoveTransformComponent(currentPosition, toPosition, this.duration,
            () => {
                this.hasFinished = true
            }))
    }
    //Method to run on every frame
    update(dt: number): void {
    }
    //Method to run at the end
    onFinish(): void {
    }
}