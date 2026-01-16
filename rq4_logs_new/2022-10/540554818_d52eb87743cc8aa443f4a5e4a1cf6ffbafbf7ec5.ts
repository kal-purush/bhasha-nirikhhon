import { Cell } from "../Cell";
import { Colors } from "../Colors";
import { Figure, FigureNames } from "./Figure";
// import blackLogo from "../../public/black-king.png";
// import whiteLogo from "../../public/white-king.png";

const blackLogo = require("../../assets/img/black-king.png");
const whiteLogo = require("../../assets/img/white-king.png");

export class King extends Figure {
  constructor(color: Colors, cell: Cell) {
    super(color, cell);
    this.logo = color === Colors.BLACK ? blackLogo : whiteLogo;
    this.name = FigureNames.KING;
  }

  canMove(target: Cell): boolean {
    if (!super.canMove(target)) {
      return false;
    }

    if (
      (target.x === this.cell.x && target.y === this.cell.y - 1) ||
      (target.x === this.cell.x - 1 && target.y === this.cell.y) ||
      (target.x === this.cell.x - 1 && target.y === this.cell.y - 1) ||
      (target.x - 1 === this.cell.x && target.y === this.cell.y) ||
      (target.x === this.cell.x && target.y - 1 === this.cell.y) ||
      (target.x - 1 === this.cell.x && target.y - 1 === this.cell.y) ||
      (target.x + 1 === this.cell.x && target.y - 1 === this.cell.y) ||
      (target.x === this.cell.x + 1 && target.y === this.cell.y - 1)
    ) {
      return true;
    }

    return false;
  }
}