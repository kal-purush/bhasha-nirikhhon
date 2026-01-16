interface GameLocation {
  x: number;
  y: number;
}

interface GameEntityOptions {
  location:GameLocation;
}


class GameEntity {

  location:GameLocation;

  constructor(options:GameEntityOptions) {
    this.location = options.location;
  }

}

class SnakeBody extends GameEntity {
}

class Food extends GameEntity{
  location:GameLocation;
}

class GameEntityGroup {
  entities:GameEntity[];

  constructor(entities:GameEntity[]) {
    this.entities = entities;
  }
}

class Snake extends GameEntityGroup {
  entities:SnakeBody[];
}

class FoodItems extends GameEntityGroup {
  entities:Food[];
}

class Game {
  protected canvas:HTMLCanvasElement;
  protected collisionManager:CollisionManager;
  protected entityGroups:GameEntityGroup[];

  protected onTick() {

  }

  addEntityGroup(entityGroup:GameEntityGroup, renderer:RendererInterface) {
  }

  addGroupCollisionEvent(groupA:GameEntityGroup, groupB:GameEntityGroup, cb:(CollisionEvent)=>void) {
    this.collisionManager.addGroupEvent(groupA, groupB, cb);
  }

  broadcast(topic, event:Object) {

  }
}

interface RendererInterface {
  render(entity:GameEntity, ctx:CanvasRenderingContext2D);
}

interface CollisionEvent {
  entities:GameEntity[];
  location:GameLocation;
}

class CollisionManager {
  addGroupEvent(groupA:GameEntityGroup, groupB:GameEntityGroup, cb:(evt:CollisionEvent)=>void) {

  }
}


class SnakeGame extends Game {

}

var game = new SnakeGame();

var snake = new Snake([
  new SnakeBody({
    location: { x: 50, y: 50 }
  })
]);

var foodItems = new FoodItems([
  new Food({
    location: { x: 10, y: 80 }
  })
]);

var snakeRenderer:RendererInterface = {
  render: function(snake:SnakeBody, ctx:CanvasRenderingContext2D) {

  }
};

var foodRenderer:RendererInterface = {
  render: function(food:Food, ctx:CanvasRenderingContext2D) {

  }
};

game.addEntityGroup(snake, snakeRenderer);
game.addEntityGroup(foodItems, foodRenderer);


game.addGroupCollisionEvent(snake, foodItems, function(evt:CollisionEvent) {
  game.broadcast('snake:grow', { location: evt.location });
});
