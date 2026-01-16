import * as Phaser from "phaser";
import { IScore } from "../../../server/Ship/interfaces/IScore";

export class ScoreView extends Phaser.GameObjects.Container {
  private blueScoreText: Phaser.GameObjects.Text;
  private redScoreText: Phaser.GameObjects.Text;

  constructor(
    scene: Phaser.Scene,
    x?: number,
    y?: number,
    children?: Phaser.GameObjects.GameObject[]
  ) {
    super(scene);

    this.blueScoreText = this.scene.add.text(16, 16, "", {
      fontSize: "32px",
      fill: "#0000FF"
    });
    this.redScoreText = this.scene.add.text(584, 16, "", {
      fontSize: "32px",
      fill: "#FF0000"
    });

    scene.add.existing(this.blueScoreText);
    scene.add.existing(this.redScoreText);
  }

  scoreUpdate = (scores: IScore[]): void => {
    this.blueScoreText.setText(
      "Blue: " + scores.find(s => s.team === "blue").quantity
    );
    this.redScoreText.setText(
      "Red: " + scores.find(s => s.team === "red").quantity
    );
  };
}