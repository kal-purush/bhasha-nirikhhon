import { UI } from "../UI.js";
import MapUI from "../worldScreen/MapUI.js";
import TileSidebar from "../worldScreen/TileSidebar.js";
import GridCoordinates from "../../world/GridCoordinates.js";
import Game from "../../Game.js";
import InventorySidebar from "../worldScreen/InventorySidebar.js";
import WorldScreenHeader from "../worldScreen/WorldScreenHeader.js";
import { Page } from "../GameWindow.js";
import QuestIndicator from "../worldScreen/QuestIndicator.js";
import { NamedTileType } from "../../world/Tile.js";

/* an alternate version of the world screen used for the ending animation
 * in which a "wave" of circuit tiles spreads out from the monolith
 */
export default class EndingWorldScreen implements Page {

    private mapUI: MapUI;
    private inventorySidebar: InventorySidebar;
    private tileSidebar: TileSidebar;
    private header: WorldScreenHeader;
    private questIndicator: QuestIndicator;
    readonly html: HTMLElement;

    constructor(private run: Game, private center: GridCoordinates, private circuitsTile: NamedTileType) {
        this.mapUI = new MapUI(this, run.world);
        this.tileSidebar = new TileSidebar(this, run);
        this.tileSidebar.changeTile(center);
        this.inventorySidebar = new InventorySidebar(run);
        this.header = new WorldScreenHeader(run, "disabled");
        this.questIndicator = new QuestIndicator(run);

        this.html = UI.makeDivContaining([

            this.header.html,
            this.questIndicator.html,

            UI.makeDivContaining([
                this.inventorySidebar.html,
                UI.makeDivContaining([this.mapUI.html], ["world-screen-map-box"]),
                this.tileSidebar.html,
            ], ["world-screen-hbox"]),

        ], ["flex-vertical", "world-screen"]);
    }

    // the animation for the end of the game shows the "circuits" tiles spreading out from the monolith and consuming the world
    expandCircuits(radius: number): void {
        const world = this.run.world;
        const tilesInRadius = world.getTilesInCircle(this.center, radius);
        const targets = tilesInRadius;
        const centerTile = world.getTileAtCoordinates(this.center);
        for (const target of targets) {
            if (target !== centerTile) {
                world.placeTile(new this.circuitsTile(target.position));
            }
        }

        this.run.updateQuestState();
        this.run.inventory.applyPopulationCaps();
        this.refresh();

        if (tilesInRadius.length < (world.width * world.height)) {
            setTimeout(() => {
                this.expandCircuits(radius * 1.01 + 0.05);
            }, 100);
        } else {
            for (const resource of this.run.inventory.getResourceList()) {
                // once the entire world is consumed, all resources should disappear
                this.run.inventory.removeResource(resource, this.run.inventory.getResourceQuantity(resource));
            }
            this.refresh();
        }
    }

    refresh(): void {
        this.mapUI.refresh();
        this.tileSidebar.refresh();
        this.inventorySidebar.refresh();
        this.header.refresh();
        this.questIndicator.refresh();
    }

    // keyboard events for this page are passed to the map ui, which uses arrow keys or wasd to move around the map
    handleKeyDown(ev: KeyboardEvent): void {
        this.mapUI.handleKeyDown(ev);
    }

    changeSidebarTile(position: GridCoordinates | null): void {
        this.tileSidebar.changeTile(position);
    }
}