import WorldPredicate from "./WorldPredicate.js";
import Game from "../Game.js";
import Cost from "../resources/Cost.js";
import Resource from "../resources/Resource.js";
import Tile from "../world/Tile.js";

export class MinResourcePredicate extends WorldPredicate {
    constructor(
        private resource: Resource,
        private minQuantity: number,
    ){
        super();
    }

    evaluate(run: Game): boolean {
        return run.inventory.getResourceQuantity(this.resource) >= this.minQuantity;
    }
    toString(): string {
        return `have at least ${this.minQuantity} units of ${this.resource.name} in inventory`;
    }
}

export class MinTilePredicate extends WorldPredicate {
    constructor(
        private tileType: typeof Tile & { readonly tileName: string },
        private minQuantity: number,
    ){
        super();
    }

    evaluate(run: Game): boolean {
        const matchingTiles = run.world.getTiles().filter(tile => tile instanceof this.tileType);
        return (matchingTiles.length) >= this.minQuantity
    }
    toString(): string {
        const plural:string = (this.minQuantity == 1) ? "" : "s";
        return `have at least ${this.minQuantity}  ${this.tileType.tileName} tile${plural}`;
    }
}