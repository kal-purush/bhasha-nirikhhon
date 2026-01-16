/// <reference types="snapsvg" />
import SvgComponent from "./svg-component";
import Position from "./position";
import RectangleDimensions from "./rectangle-dimensions";
import MapPinDetails from "./map-pin-details";
import Snap = require("snapsvg");
export default class MapPinCardComponent implements SvgComponent {
    private details;
    snapCtx: Snap.Paper;
    position: Position;
    dimensions: RectangleDimensions;
    constructor(details: MapPinDetails, snapCtx: Snap.Paper, position: Position, dimensions: RectangleDimensions);
    draw(): Promise<void>;
}