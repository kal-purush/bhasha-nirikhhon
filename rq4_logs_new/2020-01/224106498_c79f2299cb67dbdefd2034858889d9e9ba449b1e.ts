import Tile from "../Tile.js";
import TileProject from "../../tileProjects/TileProject.js";
import GridCoordinates from "../GridCoordinates.js";
import Species from "../../resources/Species.js";
import Game from "../../Game.js";
import Wasteland from "./Wasteland.js";
import Habitat from "./Habitat.js";
import MiningFacility from "./MiningFacility.js";
import SolarPanels from "./SolarArray.js";

export default class Lander extends Tile {

    possibleProjects: TileProject[] = [
        new TileProject(
            "Unpack Lander",
            (position: GridCoordinates, run: Game) => {
                const world = run.world;

                // place colony buildings
                world.placeTile(new Habitat(new GridCoordinates(position.x + 1, position.y)));
                world.placeTile(new MiningFacility(new GridCoordinates(position.x, position.y - 1)));
                world.placeTile(new SolarPanels(new GridCoordinates(position.x + 1, position.y - 1)));

                // remove pod
                world.placeTile(new Wasteland(position));

                // create initial population
                run.inventory.addWorkers(Species.Human, 100);
            },
            [],
            [],
        ),
    ];

    constructor(position: GridCoordinates) {
        super(position);
    }

    getImgSrc(): string {
        return "assets/tiles/lander.png";
    }

    static readonly tileName: string = "Landing Pod";
    getTileName(): string {
        return Lander.tileName;
    }
}