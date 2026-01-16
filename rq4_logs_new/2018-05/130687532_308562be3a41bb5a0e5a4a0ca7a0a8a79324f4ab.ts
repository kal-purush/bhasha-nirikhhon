/// <reference path="../strategies/move_behaviours/move_behaviour.ts"/>

abstract class MoveableObject extends GameObject {
  private _moveBehaviour!: MoveBehaviour;

  get moveBehaviour(): MoveBehaviour { return this._moveBehaviour }
  set moveBehaviour(behaviour: MoveBehaviour) { this._moveBehaviour = behaviour }

  constructor(x: number, y: number) {
    super(x, y, 'movable_object');
  }

  public move(): void {

  }
}