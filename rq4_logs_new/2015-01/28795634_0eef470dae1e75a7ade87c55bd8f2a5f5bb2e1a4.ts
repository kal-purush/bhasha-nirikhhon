/// <reference path="../../typings/underscore/underscore.d.ts" />

/**
 * Descriptive information about a game entity group
 */
interface GameEntityGroupMeta {
  entityGroup:GameEntityGroup;
  renderer: RendererInterface;
  controller:ControllerInterface;
}

class Game {
  protected canvas:HTMLCanvasElement;
  protected collisionManager:CollisionManagerInterface;
  /** Describes the entity groups of which the game is aware. */
  protected groupsMeta:GameEntityGroupMeta[] = [];

  protected onTick() {
    this.collisionManager.check();

    _.each(this.groupsMeta, (meta:GameEntityGroupMeta) => {
      this.renderEntityGroup(meta.entityGroup, meta.renderer);
      meta.entityGroup.forEach(entity => entity.onTick());
    });
  }

  protected renderEntityGroup(entityGroup:GameEntityGroup, renderer:RendererInterface) {
    entityGroup.forEach(entity => {
      renderer.render(entity, this.getContext());
    });
  }

  protected getContext() {
    return this.canvas.getContext('2d');
  }

  addEntityGroup(entityGroup:GameEntityGroup, renderer:RendererInterface, controller:ControllerInterface) {
    this.bootstrapEntityGroup({
      entityGroup: entityGroup,
      renderer: renderer,
      controller: controller
    });
  }

  protected bootstrapEntityGroup(entityGroupMeta:GameEntityGroupMeta) {
    this.groupsMeta.push(entityGroupMeta);

    // setup whatever controller event handling we need,
    // like subscribing to input/game events, etc.
  }

  addGroupCollisionEvent(groupA:GameEntityGroup, groupB:GameEntityGroup, cb:(CollisionEvent)=>void) {
    this.collisionManager.addGroupEvent(groupA, groupB, cb);
  }

  broadcast(topic, event:GameEventInterface) {

  }

  subscribe(topic, cb:(event:GameEventInterface)=>void) {

  }


}

export = Game;