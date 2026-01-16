import GridCoordinates from "../world/GridCoordinates";
import Game from "../Game";
import { TilePredicate, hasTech, adjacentToRoad, tileWithinDistance } from "./predicates";
import Technology from "../techtree/Technology";
import { NamedTileType } from "../world/Tile";
import Monolith from "../world/Tiles/Monolith";
import HumanMonolith from "../world/Tiles/HumanMonolith";
import Species from "../resources/Species";
import Ruins from "../world/Tiles/Ruins";

// a requirement for completing a tileproject
export default class DescribedTilePredicate {
    constructor(
        // user-readable label describing the requirement
        readonly description: string,
        // predicate that returns true when the requirement is met
        readonly evaluate: TilePredicate,
    ) { }

    toString(): string {
        return this.description;
    }
}

export function techRequirement(technology: Technology): DescribedTilePredicate {
    return new DescribedTilePredicate(`have the ${technology.name} technology`, hasTech(technology));
}

export const roadRequirement = new DescribedTilePredicate("adjacent to a road", adjacentToRoad);

export function tileWithinDistanceRequirement(tileType: NamedTileType, radius: number): DescribedTilePredicate {
    return new DescribedTilePredicate(
        radius === 1 ? `adjacent to a ${tileType.tileName}` : `within ${radius} units of a ${tileType.tileName}`,
        tileWithinDistance(tileType, radius),
    );
}

export function nearMonolithRequirement(radius: number): DescribedTilePredicate {
    return new DescribedTilePredicate(
        radius === 1 ? `adjacent to a Monolith` : `within ${radius} units of a Monolith`,
        (game: Game, position: GridCoordinates) => {
            return tileWithinDistance(Monolith, radius)(game, position) || tileWithinDistance(HumanMonolith, radius)(game, position);
        }
    );
}

export function nearRuinsOrMonolith(radius: number): DescribedTilePredicate {
    return new DescribedTilePredicate(
        radius === 1 ? `adjacent to ${Ruins.tileName} or ${Monolith.tileName}`
            : `within ${radius} units of ${Ruins.tileName} or ${Monolith.tileName}`,
        (game: Game, position: GridCoordinates) => {
            return tileWithinDistance(Ruins, radius)(game, position)
                    || tileWithinDistance(Monolith, radius)(game, position)
                    || tileWithinDistance(HumanMonolith, radius)(game, position);
        }
    );
}

export function speciesPopulationRequirement(species: Species, minPopulation: number): DescribedTilePredicate {
    return new DescribedTilePredicate(
        `have at least ${minPopulation} ${species.name}`,
        (game: Game) => (game.inventory.getPopulation(species) >= minPopulation),
    );
}