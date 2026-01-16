import Tile from "../Tile.js";
import TileProject from "../../tileProjects/TileProject.js";
import GridCoordinates from "../GridCoordinates.js";
import { IndustryConstructionTexture } from "../../UI/Images.js";
import Game from "../../Game.js";
import Resource from "../../resources/Resource.js";
import Cost from "../../resources/Cost.js";
import { TileWithinDistancePredicate, adjacentToRoad } from "../../predicates/TilePredicates.js";
import Wasteland from "./Wasteland.js";
import { MonolithSurveyTech, NanoTech, NeuralUploadingTech } from "../../techtree/TechTree.js";
import { TechPredicate } from "../../predicates/WorldPredicates.js";
import { constructionProject } from "../../tileProjects/TileProject.js";
import NanotechFoundry from "./NanotechFoundry.js";
import Monolith from "./Monolith.js";
import NeuralEmulator from "./NeuralEmulator.js";

export default class ConstructionVictory extends Tile {

    protected texture: HTMLImageElement = IndustryConstructionTexture;

    constructor(position: GridCoordinates) {
        super(position);
    }

    possibleProjects: TileProject[] = [
        new TileProject("Break down construction site", "Revert this location to wasteland",
            (position: GridCoordinates, run: Game) => {
                run.world.placeTile(new Wasteland(position));
            }, [], [], [],
        ),

        constructionProject(NanotechFoundry,
            [new Cost(Resource.SmartMatter, 1000), new Cost(Resource.BuildingMaterials, 500), new Cost(Resource.Electronics, 200)],
            [adjacentToRoad, new TileWithinDistancePredicate(3, Monolith), new TechPredicate(NanoTech)],
            [new TechPredicate(MonolithSurveyTech)],
        ),

        constructionProject(NeuralEmulator,
            [new Cost(Resource.BuildingMaterials, 300), new Cost(Resource.Electronics, 1000), new Cost(Resource.Superconductor, 1000), new Cost(Resource.SmartMatter, 500)],
            [adjacentToRoad, new TileWithinDistancePredicate(2, Monolith), new TechPredicate(NeuralUploadingTech)],
            [new TechPredicate(MonolithSurveyTech)],
        ),
    ];

    static readonly tileName: string = "Construction Site - Hyperengineering";
    static readonly tileDescription: string = "An area designated for construction of advanced technologies necessary for completing the mission";
    getTileName(): string {
        return ConstructionVictory.tileName;
    }
    getTileDescription(): string {
        return ConstructionVictory.tileDescription;
    }
}