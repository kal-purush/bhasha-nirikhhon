import * as io from "socket.io-client";
import * as Phaser from "phaser";

interface IShip extends Phaser.Physics.Arcade.Image {
  playerId?: string;
  oldPosition?: {
    x: number;
    y: number;
    rotation: number;
  };
}

interface IOtherShip extends Phaser.GameObjects.Image {
  playerId?: string;
}

export class MainGame extends Phaser.Scene {
  private socket: SocketIO.Socket;
  private otherPlayers: Phaser.Physics.Arcade.Group;
  private cursors: Phaser.Input.Keyboard.CursorKeys;
  private blueScoreText: Phaser.GameObjects.Text;
  private redScoreText: Phaser.GameObjects.Text;
  private ship: IShip | any;
  private star: Phaser.Physics.Arcade.Image;

  constructor() {
    super({ key: "MainGame" });
  }

  public preload(): void {
    this.load.image("ship", "assets/spaceShips_001.png");
    this.load.image("otherPlayer", "assets/enemyBlack5.png");
    this.load.image("star", "assets/star_gold.png");
  }

  public create(): void {
    const self = this;
    this.socket = io();
    this.otherPlayers = this.physics.add.group();
    this.cursors = this.input.keyboard.createCursorKeys();

    this.socket.on("currentPlayers", players => {
      players.forEach(player => {
        player.playerId === self.socket.id
          ? this.addPlayer(player)
          : this.addOtherPlayers(player);
      });
    });

    this.socket.on("newPlayer", playerInfo => {
      this.addOtherPlayers(playerInfo);
    });

    this.socket.on("playerMoved", playerInfo => {
      self.otherPlayers.getChildren().forEach((otherPlayer: IShip) => {
        if (playerInfo.playerId === otherPlayer.playerId) {
          otherPlayer.setRotation(playerInfo.rotation);
          otherPlayer.setPosition(playerInfo.x, playerInfo.y);
        }
      });
    });

    this.socket.on("userDisconnect", playerId => {
      self.otherPlayers.getChildren().forEach((otherPlayer: IOtherShip) => {
        if (playerId === otherPlayer.playerId) {
          otherPlayer.destroy();
        }
      });
    });

    this.blueScoreText = this.add.text(16, 16, "", {
      fontSize: "32px",
      fill: "#0000FF"
    });
    this.redScoreText = this.add.text(584, 16, "", {
      fontSize: "32px",
      fill: "#FF0000"
    });

    this.socket.on("scoreUpdate", scores => {
      self.blueScoreText.setText(
        "Blue: " + scores.find(s => s.team === "blue").quantity
      );
      self.redScoreText.setText(
        "Red: " + scores.find(s => s.team === "red").quantity
      );
    });

    this.socket.on("starLocation", starLocation => {
      if (self.star) self.star.destroy();
      self.star = self.physics.add.image(
        starLocation.x,
        starLocation.y,
        "star"
      );
      self.physics.add.overlap(
        self.ship,
        self.star,
        () => {
          this.socket.emit("starCollected");
        },
        null,
        self
      );
    });
  }

  public update(): void {
    if (this.ship) {
      if (this.cursors.left.isDown) {
        this.ship.setAngularVelocity(-150);
      } else if (this.cursors.right.isDown) {
        this.ship.setAngularVelocity(150);
      } else {
        this.ship.setAngularVelocity(0);
      }

      if (this.cursors.up.isDown) {
        this.physics.velocityFromRotation(
          this.ship.rotation + 1.5,
          100,
          this.ship.body.acceleration
        );
      } else {
        this.ship.setAcceleration(0);
      }

      this.physics.world.wrap(this.ship, 5);

      const x = this.ship.x;
      const y = this.ship.y;
      const rotation = this.ship.rotation;

      if (
        this.ship.oldPosition &&
        (x !== this.ship.oldPosition.x ||
          y !== this.ship.oldPosition.y ||
          rotation !== this.ship.oldPosition.rotation)
      ) {
        this.socket.emit("playerMovement", { x, y, rotation });
      }

      this.ship.oldPosition = {
        x,
        y,
        rotation
      };
    }
  }

  private addPlayer(playerInfo): void {
    this.ship = this.physics.add
      .image(playerInfo.x, playerInfo.y, "ship")
      .setOrigin(0.5, 0.5)
      .setDisplaySize(53, 40);
    if (playerInfo.team === "blue") {
      this.ship.setTint(0x0000ff);
    } else {
      this.ship.setTint(0xff0000);
    }
    this.ship.setDrag(100);
    this.ship.setAngularDrag(100);
    this.ship.setMaxVelocity(200);
  }

  private addOtherPlayers(playerInfo): void {
    const otherPlayer: IOtherShip = this.add
      .sprite(playerInfo.x, playerInfo.y, "otherPlayer")
      .setOrigin(0.5, 0.5)
      .setDisplaySize(53, 40);
    if (playerInfo.team === "blue") {
      otherPlayer.setTint(0x0000ff);
    } else {
      otherPlayer.setTint(0xff0000);
    }
    otherPlayer.playerId = playerInfo.playerId;
    this.otherPlayers.add(otherPlayer);
  }
}