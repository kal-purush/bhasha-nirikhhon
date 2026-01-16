import Tile from "../Tile.js";
import GridCoordinates from "../GridCoordinates.js";
import Resource from "../../resources/Resource.js";
import Conversion from "../../resources/Conversion.js";
import Cost from "../../resources/Cost.js";
import { ConstructionFactoryTexture } from "../../UI/Images.js";

export default class ConstructionFactory extends Tile {
    protected texture: HTMLImageElement = ConstructionFactoryTexture;

    constructor(position: GridCoordinates) {
        super(position);
    }

    resourceConversions = [
        new Conversion(
            [new Cost(Resource.Metal, 1000)],
            [new Cost(Resource.BuildingMaterials, 1000)],
            250,
        ),
    ];

    static readonly tileName: string = "Construction Materials Factory";
    static readonly tileDescription: string = `A facility for producing ${Resource.BuildingMaterials}`;
    getTileName(): string {
        return ConstructionFactory.tileName;
    }
    getTileDescription(): string {
        return ConstructionFactory.tileDescription;
    }
}