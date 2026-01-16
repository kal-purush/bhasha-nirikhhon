import {Carrier, Cruiser, Destroyer, Frigate, Warship} from "..";
import {InvalidGridSizeException} from "../exception/InvalidGridSizeException";

export const SMALL_SIZE_GRID = 5;
export const MEDIUM_SIZE_GRID = 10;
export const LARGE_SIZE_GRID = 15;

export class GameConfig {
    public gridSize: number = 10;
    private _warshipPool: Warship[];

    static getDefault() {
        return new GameConfig();
    }

    get warshipPool(): Warship[] {
        if (!this._warshipPool) {
            this._buildWarshipPool();
        }
        return this._warshipPool;
    }

    set warshipPool(value: Warship[]) {
        this._warshipPool = value;
    }

    private _buildWarshipPool() {
        if (3 <= this.gridSize && this.gridSize <= SMALL_SIZE_GRID) {
            this._warshipPool = [
                new Destroyer(),
                new Frigate()
            ];
        } else if (this.gridSize <= MEDIUM_SIZE_GRID) {
            this._warshipPool = [
                new Carrier(),
                new Destroyer(),
                new Cruiser(),
                new Frigate(),
            ];
        } else if (this.gridSize >= LARGE_SIZE_GRID) {
            this._warshipPool = [
                new Carrier(),
                new Destroyer(),
                new Destroyer(),
                new Cruiser(),
                new Cruiser(),
                new Frigate(),
                new Frigate(),
                new Frigate(),
            ];
        } else {
            throw new InvalidGridSizeException();
        }
    }
}