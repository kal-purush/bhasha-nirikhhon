import Tile from "../Tile.js";
import GridCoordinates from "../GridCoordinates.js";
import Resource from "../../resources/Resource.js";
import Conversion from "../../resources/Conversion.js";
import Cost from "../../resources/Cost.js";
import { RecyclerTexture1, RecyclerTexture2 } from "../../UI/Images.js";

export default class Recycler extends Tile {
    protected texture: HTMLImageElement = RecyclerTexture1;
    private textureVariant: 1 | 2;

    constructor(position: GridCoordinates, textureVariant: 1 | 2) {
        super(position);
        this.textureVariant = textureVariant;
    }

    resourceConversions = [
        new Conversion(
            [],
            [new Cost(Resource.Cavorite, 200)],
            300,
        ),
        new Conversion(
            [],
            [new Cost(Resource.Orichalcum, 200)],
            300,
        ),
    ];

    static readonly tileName: string = "Alien Ruins Recycler Excavation";
    static readonly tileDescription: string = `A facility that harvests and recycles exotic materials from the alien structure.`;
    getTileName(): string {
        return Recycler.tileName;
    }
    getTileDescription(): string {
        return Recycler.tileDescription;
    }

    getTexture(): HTMLImageElement {
        switch (this.textureVariant) {
        case 1:
            return RecyclerTexture1;
        case 2:
            return RecyclerTexture2;
        }
    }
}