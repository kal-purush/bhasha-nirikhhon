import Tile from "../Tile.js";
import GridCoordinates from "../GridCoordinates.js";
import { HumanCircuitsTexture } from "../../UI/Images.js";

export default class HumanCircuits extends Tile {
    protected texture: HTMLImageElement = HumanCircuitsTexture;

    constructor(position: GridCoordinates) {
        super(position);
    }

    static readonly tileName: string = "Hypercomputer Network";
    static readonly tileDescription: string =
    `A self-replicating computing substrate hosting a utopian virtual reality for uploaded human minds`;

    getTileName(): string {
        return HumanCircuits.tileName;
    }

    getTileDescription(): string {
        return HumanCircuits.tileDescription;
    }
}