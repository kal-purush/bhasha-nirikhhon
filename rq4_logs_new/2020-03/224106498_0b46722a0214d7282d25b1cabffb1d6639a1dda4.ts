import Tile from "../Tile.js";
import GridCoordinates from "../GridCoordinates.js";
import Species from "../../resources/Species.js";
import Housing from "../../resources/Housing.js";
import { RobotHiveTexture } from "../../UI/Images.js";
import TileProject from "../../tileProjects/TileProject.js";
import Cost from "../../resources/Cost.js";
import { ResolvePlugin } from "webpack";
import Resource from "../../resources/Resource.js";
import { TechPredicate } from "../../predicates/WorldPredicates.js";
import Game from "../../Game.js";

export default class RobotHive extends Tile {

    protected texture: HTMLImageElement = RobotHiveTexture;

    populationCapacity: Housing = new Housing(Species.Robot, 5000);

    possibleProjects: TileProject[] = [
        new TileProject("Construct robot swarm", "Creates 100 robotic worker drones",
            (position: GridCoordinates, game: Game) => {
                game.inventory.addWorkers(Species.Robot, 100);
            },
            [new Cost(Resource.Electronics, 50), new Cost(Resource.Energy, 100)],
            [], []
        ),
    ];

    constructor(position: GridCoordinates) {
        super(position);
    }

    static readonly tileName: string = "Drone Hive";
    static readonly tileDescription: string = "A facility for manufacturing, storing, and maintaining mobile robotic worker drones";
    getTileName(): string {
        return RobotHive.tileName;
    }
    getTileDescription(): string {
        return RobotHive.tileDescription;
    }
}