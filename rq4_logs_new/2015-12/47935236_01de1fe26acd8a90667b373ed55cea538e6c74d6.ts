/// <reference path="lib/lib.d.ts"/>

// TODO: parse map json (better than phaser -_-)

interface CustomTileProperties {
  powerupType: string;
}

enum HealthEvents {
  /**
   * args: previous health, new health
   */
  ChangeHealth
}

interface HasHealth {
  healthEvents: Events<HealthEvents>;
  health: number;
  maxHealth: number;
}

type Inventory = MagicDict<InventoryContents, number>;

class G {
  static ship: Ship;
  static inventory: MagicDict<InventoryContents, number>;

  static map: TiledMapParser;
  static hud: HUD;
  static explosionMaker: ParticleExplosionMaker;
  static staticEmitter: ParticleEmitter;

  static get walls(): Group<Sprite> {
    return new Group(G.map.getTileLayer("Wall").children);
  }
}

enum InventoryContents {
  Token,
  Fuel,
  DamageIncrease,
  DoubleJump
}

// We are using health as fuel here

class Ship extends Sprite implements HasHealth {
  public inventory: Inventory;

  healthEvents: Events<HealthEvents>;
  health: number;
  maxHealth: number;

  /**
   * Note: Pass in cloned inventory rather than original.
   * 
   * @param inventory
   */
  constructor(inventory: Inventory) {
    super("assets/ship.png");

    this.healthEvents = new Events<HealthEvents>();
    this.inventory = new MagicDict<InventoryContents, number>(() => 0);

    this.x = 200;
    this.y = 200;
  }

  addItemToInventory(item: InventoryContents): void {
    this.inventory.put(item, this.inventory.get(item) + 1);
  }

  update(): void {
    
  }
}

class Pickup extends Sprite {
  pickup(): void {
    G.explosionMaker.explodeAt(this.globalX, this.globalY);

    this.destroy();
  }

  update(): void {
    super.update();

    if (Math.random() > .9) {
      G.staticEmitter.emitIn(this.globalBounds);
    }
  }
}

class DoubleJump extends Pickup {
  constructor() {
    super("assets/token.png");
  }

  public pickup(): void {
    super.pickup();

    G.ship.addItemToInventory(InventoryContents.Token);
  }
}

class PowerIncrease extends Pickup {
  constructor() {
    super("assets/powerincrease.png");
  }

  public pickup(): void {
    super.pickup();

    G.ship.addItemToInventory(InventoryContents.DamageIncrease);
  }
}

class HealthBar extends Sprite {
  private _barWidth: number = 100;
  private _barHeight: number = 15;

  private _healthbarRed: Sprite;
  private _healthbarGreen: Sprite;
  private _healthbarText: TextField;

  private _showText: boolean = false;

  private _target: HasHealth;

  constructor(target: HasHealth, width: number = 100, height: number = 15) {
    super();

    this._target = target;
    this._barWidth = width;
    this._barHeight = height;

    this.createHealthbar();

    target.healthEvents.on(HealthEvents.ChangeHealth, (prevHealth: number, currentHealth: number) => {
      this.tween.addTween("animate-healthbar", 15, (e: Tween) => {
        this.animateHealthbar(e, prevHealth, currentHealth);
      })
    });
  }

  animateHealthbar(e: Tween, prevHealth: number, currentHealth: number): void {
    const prevWidth: number = (prevHealth / this._target.maxHealth) * this._barWidth;
    const nextWidth: number = (currentHealth / this._target.maxHealth) * this._barWidth;

    this._healthbarGreen.width = Util.Lerp(prevWidth, nextWidth, e.percentage);
  }

  createHealthbar(): void {
    this._healthbarRed = new Sprite("assets/healthbar_red.png")
      .moveTo(10, 10)
      .setZ(4)
      .setDimensions(this._barWidth, this._barHeight)
      .addTo(this);

    this._healthbarGreen = new Sprite("assets/healthbar_green.png")
      .moveTo(10, 10)
      .setZ(5)
      .setDimensions(this._barWidth, this._barHeight)
      .addTo(this);

    if (this._showText) {
      this._healthbarText = new TextField("10/10")
        .setDefaultTextStyle({ font: "12px Verdana", fill: "white" })
        .moveTo(12, 10)
        .setZ(6)
        .addTo(this);
    }
  }

  update(): void {
    super.update();
  }
}


class Fuel extends Pickup {
  constructor() {
    super("assets/fuel.png");
  }

  public pickup(): void {
    super.pickup();

    G.ship.addItemToInventory(InventoryContents.Fuel);
  }
}

class IconAndText extends Sprite {
  _icon: Sprite;
  _text: TextField;

  constructor(iconPath: string) {
    super();

    this._icon = new Sprite(iconPath).addTo(this);

    this._icon.x = 0;
    this._icon.y = 0;

    this._text = new TextField("??").addTo(this);

    this._text.x = 40;
    this._text.y = 0;

    this.z = 10;
  }

  setText(text: string): void {
    this._text.text = text;
  }
}

@component(new FixedToCamera(0, 0))
class HUD extends Sprite {
  private _fuel:   IconAndText;
  private _tokens: IconAndText;

  constructor() {
    super();

    this.z = 20;

    this._fuel      = new IconAndText("assets/fuel.png") .moveTo(120, 10).addTo(this);
    this._tokens    = new IconAndText("assets/token.png").moveTo(210, 10).addTo(this);
  }

  update(): void {
    this._fuel.setText(String(G.ship.inventory.get(InventoryContents.Fuel)));
    this._tokens.setText(String(G.ship.inventory.get(InventoryContents.Token)));
  }
}

class MyGame extends Game {
  constructor() {
    super(600, 400, document.getElementById("main"), 0x000000, true);

    G.map = new TiledMapParser("assets/map.json")
      .addObjectParser(22, (texture, json) => {
        console.log("yay!");

        return null;
      })
      .parse();
  }

  loadingComplete(): void {
    G.inventory = new MagicDict<InventoryContents, number>();

    G.inventory.put(InventoryContents.Token, 1)
    G.inventory.put(InventoryContents.Fuel, 2)

    G.ship = new Ship(G.inventory.clone());

    Globals.camera.x = Globals.stage.width / 2;
    Globals.camera.y = Globals.stage.height / 2;

    G.hud = new HUD();
    Globals.stage.addChild(G.hud);

    Globals.stage.addChild(G.ship);

    G.map.z = -10;

    Globals.stage.addChild(G.map);

    G.explosionMaker = new ParticleExplosionMaker("assets/particles.png", 16, 16, 64, 16);
    G.staticEmitter = new ParticleEmitter("assets/particles.png", 16, 16, 64, 16);

    Globals.stage.addChild(G.explosionMaker);
    Globals.stage.addChild(G.staticEmitter);

    new FPSCounter();

    super.loadingComplete();
  }
}

Util.RunOnStart(() => {
  Debug.DEBUG_MODE = false;

  new MyGame();
});