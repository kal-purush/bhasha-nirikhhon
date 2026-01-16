// https://api.mapy.cz/doc/symbols/SMap.html#constructor

//*****************************************************
//****** Základ ***************************************
//*****************************************************

class SMap {
    // Property
    static TRANSFORM:string;
    static Vector:Function;

    // Constructor
    constructor(container: Node, center?: SMap.Coords, zoom?: number, options?: {
        minZoom?: number; maxZoom?: number; orientation: number; projection: number; animTime?: number; zoomTime?: number; rotationTime?: number;
    });

    // Destructor
    $destructor(): void;

    // Method
    addCard(card: SMap.Card, coords: SMap.Coords, noPan: boolean): void;
    addControl(c: SMap.Control, placement?: Object): void;
    addDefaultContextMenu(): void;
    addDefaultControls(options): void;
    addDefaultLayer(id: string): SMap.Layer;
    addDefaultLayer(id: number): SMap.Layer;
    addLayer(l: SMap.Layer, before?: boolean);
    adjustCoordsByPadding(coords: SMap.Coords, zoom?: number, projection?: SMap.Projection): SMap.Coords;
    animate(c: SMap.Coords): void;
    animate(c: SMap.Pixel): void;
    computeCenterZoom(arr: SMap.Coords[], usePadding?: boolean): Array;
    createDefaultDataProvider(): void;
    formatString(template: string, customValues?: Object): string;
    getCard(): SMap.Card;
    getCenter(): SMap.Coords;
    getContainer(): Node;
    getContent(): Node;
    getControls(): SMap.Control[];
    getGeometryCanvas(): JAK.Vector;
    getLayer(id: string): SMap.Layer;
    getLayer(id: number): SMap.Layer;
    getLayerContainer(type: string): Node;
    getLayerContainer(type: number): Node;
    getMap(): SMap;
    getOffset(): SMap.Pixel;
    getOrientation(): number;
    getPadding(which: string): number;
    getProjection(): SMap.Projection;
    getSignals(): JAK.Signals;
    getSize(): SMap.Pixel;
    getZoom(): number;
    getZoomRange(): number[];
    isObliqueAvailable(coords: SMap.Coords): boolean;
    isOphotoAvailable(coords: SMap.Coords): boolean;
    lock(): void;
    redraw(): void;
    removeCard(): void;
    removeControl(c: SMap.Control): void;
    removeLayer(l: SMap.Layer): void;
    setCenter(c: SMap.Coords): void;
    setCenter(c: SMap.Pixel): void;
    setCenterZoom(c: SMap.Coords, z: number): void;
    setCursor(cursor: string, x?: number, y?: number): void;
    setOrientation(o: number, animate?: boolean): void;
    setPadding(which: string, value: number): void;
    setProjection(projection: SMap.Projection): void;
    setZoom(z: number, fixedPoint?: SMap.Coords, animate?: boolean): void;
    setZoom(z: string, fixedPoint?: SMap.Coords, animate?: boolean): void;
    setZoomRange(min: number, max: number): void;
    syncPort(): void;
    unlock(): void;
    zoomAnimationStart(fixedPoint: SMap.Coords): void;
    zoomAnimationStep(fracZoom: number): void;
    zoomAnimationStop(): void;
    zoomAnimationTarget(targetZoom: number, sourceZoom?: number): void;
}

declare module SMap {

    export var DEF_BASE:number;
    export var DEF_BIKE:number;
    export var DEF_GEOGRAPHY:number;
    export var DEF_HISTORIC:number;
    export var DEF_HYBRID:number;
    export var DEF_OBLIQUE:number;
    export var DEF_OBLIQUE_HYBRID:number;
    export var DEF_OPHOTO:number;
    export var DEF_OPHOTO0203:number;
    export var DEF_OPHOTO0406:number;
    export var DEF_PANO:number;
    export var DEF_RELIEF:number;
    export var DEF_SMART_BASE:number;
    export var DEF_SMART_OPHOTO:number;
    export var DEF_SMART_SUMMER:number;
    export var DEF_SMART_TURIST:number;
    export var DEF_SMART_WINTER:number;
    export var DEF_SUMMER:number;
    export var DEF_TRAIL:number;
    export var DEF_TURIST:number;
    export var DEF_TURIST_WINTER:number;
    export var EAST:number;
    export var GEOMETRY_CIRCLE:number;
    export var GEOMETRY_ELLIPSE:number;
    export var GEOMETRY_PATH:number;
    export var GEOMETRY_POLYGON:number;
    export var GEOMETRY_POLYLINE:number;
    export var KB_PAN:number;
    export var KB_ZOOM:number;
    export var LAYER_ACTIVE:number;
    export var LAYER_GEOMETRY:number;
    export var LAYER_HUD:number;
    export var LAYER_MARKER:number;
    export var LAYER_SHADOW:number;
    export var LAYER_TILE:number;
    export var LAYER_VECTOR:number;
    export var MAPSET_BASE:number;
    export var MAPSET_TURIST:number;
    export var MOUSE_PAN:number;
    export var MOUSE_WHEEL:number;
    export var MOUSE_ZOOM:number;
    export var MOUSE_ZOOM_IN:number;
    export var MOUSE_ZOOM_OUT:number;
    export var NORTH:number;
    export var SOUTH:number;
    export var WEST:number;


    export class Coords {
        // Constructor
        constructor(x: number, y: number);

        // Method
        azimuth(target: Coords): number;
        clone(): Coords;
        distance(target: Coords, altitude?: number): number;
        equals(coords: Coords): boolean;
        fixedPoint(map: SMap, newZoom: number): Coords;
        inMap(map: SMap, usePadding?: boolean): boolean;
        newCenter(map: SMap, newZoom: number): Coords;
        toJTSK(): number[];
        toPixel(map: SMap, zoom?: number): Pixel;
        toPP(): number[];
        toString(): string;
        toUTM33(): number[];
        toWGS84(): number[];
        toWGS84(format: number): string[];
        wrap(): void;

        // Static method
        static fromEvent(event: event, map: SMap): Coords;
        static fromEXIF(exit: JAK.EXIF): Coords;
        static fromJTSK(x: number, y: number): Coords;
        static fromPP(PPx: number, PPy: number): Coords;
        static fromUTM33(x: number, y: number): Coords;
        static fromWGS84(lonD: number, latD: number): Coords;
    }
}


//*****************************************************
//****** Vrstvy ***************************************
//*****************************************************

declare module SMap {

    export class Layer extends IOwned {
        // Constructor
        constructor(id?: string);
        constructor(id?: number);

        // Destructor
        $destructor(): void;

        // Property
        Vector; // TODO

        // Method
        clear(): void;
        disable(); void;
        enable(): void;
        getContainer(): Object;
        getCopyright(zoom: number): string;
        getCopyright(zoom: number): string[];
        getId(): string;
        getId(): number;
        isActive(): boolean;
        redraw(full: boolean); void;
        removeAll(): void;
        rotateTo(angle); // TODO
        setCopyright(copyright: Object): void;
        supportsAnimation(): boolean;
        zoomTo(zoom, fixedPoint); // TODO
    }
}


declare module SMap.Layer {

    export class Canvas extends SMap.Layer {
        // Constructor
        constructor(id?: string, layerId: number);
        constructor(id?: number, layerId: number);
    }

    export class Geometry extends SMap.Layer {
        // Constructor
        constructor(id?: string, options?: {supportsAnimation?: boolean});
        constructor(id?: number, options?: {supportsAnimation?: boolean});

        // Method
        addGeometry(geometry: SMap.Geometry): void;
        clear(): void;
        fillFromData(dataSets: any); void;
        redraw(full: boolean): void;
        redrawGeometry(g: SMap.Geometry): void;
        removeAll(): void;
        removeGeometry(geometry: SMap.Geometry): void;
    }

    export class GPX extends SMap.Layer.Multi {
        // Constructor
        constructor(xmlDoc: xmlDoc, id: string, options?: {maxPoints?: number; colors?: string[]});

        // Method
        fit(): void;
    }

    export class HUD extends SMap.Layer {
        // Constructor
        constructor(id?: string);
        constructor(id?: number);

        // Method
        addItem(node: Node, placement: Object): void;
        clear(): void;
        enable(): void;
        removeItem(node: Node): void;
    }

    export class Image extends SMap.Layer {
        // Constructor
        constructor(id?: string);

        // Method
        addImage(url: string, leftTop: SMap.Coords, rightBottom: SMap.Coords, opacity?: number = 1): string;
        clear(): void;
        enable(): void;
        redraw(full: boolean): void;
        removeAll(): void;
        removeImage(id: string): void;
    }

    export class KML extends SMap.Layer.Multi {
        // Constructor
        constructor(xmlDoc: xmlDoc, id: string, options?: {maxPoints: number; url?: string});

        // Method
        fit(): void;
    }

    export class Marker extends SMap.Layer {
        // Constructor
        constructor(options?: {prefetch?: number; supportsAnimation?: boolean});

        // Destructor
        $destructor(): void;

        // Method
        addMarker(marker: SMap.Marker, noRedraw?: boolean): void;
        addMarker(marker: SMap.Marker[], noRedraw?: boolean): void;
        clear(): void;
        fillFromData(data: any): void;
        fillFromData(data: any[]): void;
        getMarkers(): SMap.Marker[];
        positionMarker(marker: SMap.Marker): void;
        redraw(full: boolean): void;
        removeAll(): void;
        removeMarker(marker: SMap.Marker, noRedraw?: boolean): void;
        removeMarker(marker: SMap.Marker[], noRedraw?: boolean): void;
        setClusterer(): void;
        setReposition(options: Object): void; // TODO
    }

    export class Multi extends SMap.Layer {
        // Constructor
        constructor(id: string);
        constructor(id: number);

        // Destructor
        $destructor(): void;

        // Method
        addLayer(l: SMap.Layer): void;
        clear(); void;
        disable(): void;
        enable(): void;
        getContainer(): Object;
        getLayers(): SMap.Layer[];
        redraw(full: boolean): void;
        removeAll(): void;
        removeLayer(l: SMap.Layer): void;
        setOwner(owner: SMap.IOwned): void;
    }

    export class Smart extends SMap.Layer {
        // Constructor
        constructor(id?: number);
        constructor(id?: string);

        // Method
        enable(): void;
        getContainer(); // TODO
        getLayers(); // TODO
        setHybrid(mode); // TODO
    }

    export class Tile extends SMap.Layer {
        // Constructor
        constructor(id: string, url: string, options?: {tileSize?: number; alpha?: boolean; fadeTime?: number; query?: string});

        // Method
        clear(): void;
        redraw(full: boolean): void;
        rotateTo(angle); // TODO
        setOptions(options); // TODO
        setURL(url: string): void;
        zoomTo(zoom, fixedPoint); // TODO
    }

    export class Turist extends SMap.Layer {
        // Constructor
        constructor(id?: number);
        constructor(id?: string);

        // Method
        getBike(): boolean;
        getTrail(): boolean;
        setBike(bike: boolean): void;
        setTrail(trail: boolean): void;
    }

    export class WMS extends SMap.Layer.Tile {
        // Constructor
        constructor(id: string, url: string, params: Object);
    }

    export class WMTS extends SMap.Layer.Tile {
        // Constructor
        constructor(id: string, url: string, params: Object,  options: Object);
    }
}


declare module SMap.Layer.Smart {

    export class Turist extends SMap.Layer.Smart {
        // Constructor
        constructor(id?: number);
        constructor(id?: string);

        // Method
        getBike(): boolean;
        getTrail(): boolean;
        setBike(bike: boolean): void;
        setTrail(trail: boolean): void;
    }
}


//*****************************************************
//****** Ovládací prvky *******************************
//*****************************************************

declare module SMap {

    export class Control extends IOwned {
        // Constructor
        constructor();

        // Destructor
        $destructor(): void;

        // Method
        getContainer(): Node;
        setOwner(owner: SMap): void;
    }
}


declare module SMap.Control {

    export class Compass extends SMap.Control.Visible {
        // Constructor
        constructor(options ?: {panAmount?: number; title?: string; moveThreshold?: number});

        // Method
        setOwner(owner: SMap);
    }

    export class ContextMenu extends SMap.Control {
        // Constructor
        constructor();

        // Method
        addItem(item: SMap.Control.ContextMenu.Item, pos?: number): void;
        clearItems();
        getItems(): SMap.Control.ContextMenu.Item[];
        open(event: event, coords: SMap.Coords): void;
        removeItem(item: SMap.Control.ContextMenu.Item): void;
    }

    export class Copyright extends SMap.Control.Visible {
        // Constructor
        constructor();

        // Method
        setOwner(owner: SMap): void;
    }

    export class Image extends SMap.Control.Visible {
        // Constructor
        constructor(url: string; href); // TODO

        // Method
        setOwner(owner: SMap): void;
    }

    export class Keyboard extends SMap.Control {
        // Constructor
        constructor(mode: number, options?: {panAmount?: number; focusedOnly?: boolean});

        // Method
        configure(mode); // TODO
        getMode(); // TODO
        setOwner(owner: SMap): void;
    }

    export class Layer extends SMap.Control.Visible {
        // Constructor
        constructor(options?: {width?: number; items?: number; page?: number});

        // Method
        addDefaultLayer(id: string): SMap.Layer;
        addDefaultLayer(id: number): SMap.Layer;
        addLayer(id: string, label: string, src: string, title: string): void;
        addLayer(id: number, label: string, src: string, title: string): void;
        getContent(): Node;
        setOwner(owner: SMap): void;
    }

    export class Minimap extends SMap.Control.Visible {
        // Constructor
        constructor(width: number, height: number, options?: {diff?: number; layer?: number; color?: string; opacity?: string});

        // Method
        setOptions(options: Object): void;
    }

    export class Mouse extends SMap.Control {
        // Constructor
        constructor(mode: number, options?: {scrollDelay?: number; idleDelay?: number; minDriftSpeed?: number; maxDriftSpeed?: number; driftSlowdown?: number; driftThreshold?: number});

        // Method
        configure(mode); // TODO
        getMode(); // TODO
        setOwner(owner: SMap): void;
    }

    export class Orientation extends SMap.Control.Visible {
        // Constructor
        constructor(options?: {title?: string; mode?: string});
    }

    export class Overview extends SMap.Control.Image {
        // Constructor
        constructor(url: string);
    }

    export class Rosette extends SMap.Control.Visible {
        // Constructor
        constructor(options?: {title?: string; mode?: string});

        // Method
        setOwner(owner: SMap): void;
    }

    export class Scale extends SMap.Control.Visible {
        // Constructor
        constructor();

        // Method
        setOwner(owner: SMap): void;
    }

    export class Selection extends SMap.Control.Visible {
        // Constructor
        constructor(thickness: number);
    }

    export class Sync extends SMap.Control {
        // Constructor
        constructor(options?: {bottomSpace?: number; resizeTimeout?: number});

        // Method
        setOwner(owner: SMap): void;
    }

    export class Visible extends SMap.Control {
        // Constructor
        constructor();

        // Method
        getContainer(): Node;
    }

    export class Zoom extends SMap.Control.Visible {
        // Constructor
        constructor(labels: Object, options?: {step?: string[]; titles?: string[]; sliderHeight?: string[]; showZoomMenu?: boolean;});

        // Method
        addZoomMenu(); // TODO
        removeZoomMenu(); // TODO
    }

    export class ZoomNotification extends SMap.Control.Visible {
        // Constructor
        constructor();
    }
}


declare module SMap.Control.ContextMenu {

    export class Coords extends SMap.Control.ContextMenu.Item {
        // Constructor
        constructor(label: string);
    }

    export class Item {
        // Constructor
        constructor(label: string);

        // Method
        click(e: event, menu: SMap.Control.ContextMenu);
    }

    export class Separator extends SMap.Control.ContextMenu.Item {
        // Constructor
        constructor(label: string);
    }

    export class Zoom extends SMap.Control.ContextMenu.Item {
        // Constructor
        constructor(label: string, zoomDiff: number);
    }
}


//*****************************************************
//****** Značky ***************************************
//*****************************************************

declare module SMap {

    export class Marker extends SMap.IOwned {
        // Constructor
        constructor(coords: Coords, id?: string, options?: {title?: string; size?: number[]; url?: string; anchor?: Object});

        // Destructor
        $destructor(): void;

        // Method
        click(e, elm); // TODO
        getActive(): boolean;
        getAnchor(): Pixel;
        getContainer(): Object;
        getCoords(): Coords;
        getId(); // TODO
        getSize(); // TODO
        getTitle(): string;
        setCoords(coords: Coords): void;
        setURL(url: string): void;

        static fromData(data: Object): Marker;
        static fromXML(node: Node): Marker;
    }
}


declare module SMap.Marker {

    export class Cluster extends SMap.Marker {
        // Constructor
        constructor(id?: string, options?: {color?: string; radius?: number});
        constructor(id?: number, options?: {color?: string; radius?: number});

        // Method
        addMarker(marker: Marker);
        click(e, elm); // TODO
        getMarkers(): Marker[];
        setSize(min: number, max: number): void;
    }

    export class Clusterer {
        // Constructor
        constructor(map: SMap, maxDistance?: number, clusterCtor?: Function);

        // Method
        addMarker(marker: Marker): void;
        clear(): void;
        compute(): void;
        getAllMarkers(): Marker[];
        getClusters(): Cluster[];
        getMarkers(): Marker[];
        removeMarker(marker: Marker): void;
    }
}


declare module SMap.Marker.Feature {

    export class Card extends JAK.AbstractDecorator {
        // Constructor
        constructor();

        // Method
        decorate(marker: SMap.Marker, card: SMap.Card): void;
    }

    export class Draggable extends JAK.AbstractDecorator {
        // Constructor
        constructor();

        // Method
        decorate(marker: SMap.Marker): void;
        setDrag(state: boolean): void;
    }

    export class ImageMap extends JAK.AbstractDecorator {
        // Constructor
        constructor();

        // Method
        decorate(marker: SMap.Marker, options?: {useMap ?: string; blank?: string; width?: string; height?: string}): void;
    }

    export class RelativeAnchor extends JAK.AbstractDecorator {
        // Constructor
        constructor();

        // Method
        decorate(marker: SMap.Marker, options?: {anchor ?: string; size?: Pixel;}): void;
    }

    export class Shadow extends JAK.AbstractDecorator {
        // Constructor
        constructor();

        // Method
        decorate(marker: SMap.Marker, url: string): void;
    }


}


//*****************************************************
//****** Geometrické prvky ****************************
//*****************************************************

declare module SMap {

    export class Geometry extends SMap.IOwned {
        // Constructor
        constructor(type: number, id, coords: any, options?: {
            minDist; color; opacity; width; style; outlineColor; outlineOpacity; outlineWidth; outlineStyle
        }); // TODO

        // Method
        clear(): void;
        click(e, elm); //TODO
        computeCenterZoom(map: SMap, usePadding: boolean): void;
        draw(canvas, offset); // TODO
        getCoords(); // TODO
        getId(); // TODO
        getNodes(): Node[];
        getOptions(); // TODO
        getPixels(); // TODO
        getType(); // TODO
        setCoords(coords); // TODO
        setOptions(options); // TODO
    }
}

declare module SMap.Geometry.Feature {

    export class Card extends JAK.AbstractDecorator {
        // Constructor
        constructor();

        // Method
        decorate(geometry: SMap.Geometry, card: SMap.Card);
    }
}


//*****************************************************
//****** Vizitka **************************************
//*****************************************************

declare module SMap {

    export class Card extends SMap.IOwned {
        // Constructor
        constructor(width: number);

        // Method
        anchorTo(coords: SMap.Coords): void;
        getAnchor(); // TODO
        getBody(); // TODO
        getContainer(): Node;
        getFooter(); // TODO
        getHeader(); // TODO
        isVisible(); // TODO
        makeVisible(): boolean;
        setOwner(owner); // TODO
        setSize(width: number, height: number): void;
        sync(): void;
    }
}


//*****************************************************
//****** Panorama *************************************
//*****************************************************

declare module SMap {

    export class Pano {
        // Constructor
        constructor();

        // Method
        static get(id: number): JAK.Promise;
        static getBest(coords: SMap.Coords, radius: number): JAK.Promise;
        static getNeighbors(id: number): JAK.Promise;
        static isSupported(): boolean;
    }
}


declare module SMap.Pano {

    export class Clickmask {
        // Constructor
        constructor();

        // Method
        getIndex(dx: number, dy: number): number;
        getPalette(); // TODO
    }

    export class Flash extends SMap.Pano.Renderer {
        // Constructor
        constructor();
    }

    export class Layer {
        // Constructor
        constructor();
    }

    export class Marker extends SMap.Marker {
        // Constructor
        constructor(coords: SMap.Coords, id?: string, options?: {size?: number[]; url?: string; anchor?: Object});
    }

    export class Nav {
        // Constructor
        constructor();

        // Method
        invalidate(): void;
        update(place, neighbors); // TODO
    }

    export class Place {
        // Constructor
        constructor();

        // Method

        build(gl: webgl): void;
        destroy(); void;
        draw(gl: webgl, program: webgl, scene: SMap.Pano.Scene): void;
    }

    export class Renderer {
        // Constructor
        constructor();
    }

    export class Scene {
        // Constructor
        constructor(parent: Node, options?: {
            blend?: number = 500;
            marker?: SMap.Marker = null;
            nav?: boolean = true;
            fov?: number = 1.25663706144; // π * 0.4
            fovRange?: number = [0.157079632679, 1.57079632679]; // [π / 20, π / 2]
            pitchRange?: number[] = [−0.942477796077, 0]; // [-π * 0.3, 0]
            keyboardSpeed?: number = 1.57079632679; // π / 2
        });
    }

    export class Sphere {
        // Constructor
        constructor();

        // Method
        draw(program); //TODO
        drawDebug(program); //TODO
    }

    export class Tile {
        // Constructor
        constructor(gl: webgl, position: number[], data: {vertices: number[]; indices: number[]; localUVs: number[]; globalUVs: number[]});
    }

    export class WebGL extends SMap.Pano.Renderer {
        // Constructor
        constructor();
    }
}


//*****************************************************
//****** Utility **************************************
//*****************************************************

declare module SMap {

    export class Geocoder {
        // Constructor
        constructor(query: string, callback: Function, options: Object);

        // Method
        abort(): void;
    }

    export class Pixel {
        // Constructor
        constructor(x: number, y: number);

        // Method
        clone(): Pixel;
        minus(pixel: Pixel): Pixel;
        plus(pixel: Pixel): Pixel;
        toCoords(map: SMap, zoom?: number): SMap.Coords;
        toString(): string;
        toTile(map: SMap, tileSize: number): Tile;
        wrap(map: SMap): void;

        static fromEvent(e: event, map: SMap): Pixel;
    }

    export class Route extends JAK.ISignals {
        // Constructor
        constructor(coords: SMap.Coords[], callback: Function, params?: {criterion?: string; toll?: string; avoidtraffic?: number});

        // Method
        abort(): void;
        getCoords(): SMap.Coords;
        getCriterion(): string;
        getResults(): {
            geometry; // TODO pole souřadnic geometrie trasy
            altitude; // TODO pole nadmořských výšek pro jednotlivé body geometrie
            points; // TODO pole průjezdních bodů; každý bod je další (zatím nedokumentovaný) složitý objekt
            ascent: number; // celkové stoupání
            descent: number; // celkové klesání
            url: string; // routovací adresa
            inEurope: boolean; // vede-li trasa též mimo ČR
            length: number; // celková délka trasy v metrech
            time: number; // celkový čas trasy ve vteřinách

        };
        send(): void;
    }

    export class Tile {
        // Constructor
        constructor(zoom: number, tileSize: number, x: number, y: number);

        // Method
        clone(): Tile;
        toPixel(map: SMap): Pixel;
    }

    export class Util {
        static mergeObject(from: Object, to: Object): Object;
        static stringToObject(str: string): Object;
    }
}


declare module SMap.Geocoder {

    export class Reverse extends SMap.Geocoder {
        // Constructor
        constructor(query: string, callback: Function, options: Object);
    }
}


//*****************************************************
//****** Ostatní **************************************
//*****************************************************

declare module SMap {

    export class IOwned {
        // Constructor
        constructor();

        // Method
        getMap():SMap;
        setOwner(owner: IOwned):void;
    }

    export class Projection extends IOwned {
        // Constructor
        constructor();

        // Method
        coordsToPixel(coords: SMap.Coords, center: SMap.Coords, zoom: number, orientation: number): SMap.Pixel;
        pixelToCoords(pixel: SMap.Pixel, center: SMap.Coords, zoom: number, orientation: number): SMap.Coords;
        shiftCoords(coords: SMap.Coords, zoom: number, pixel: SMap.Pixel): SMap.Coords;
    }
}

declare module SMap.Projection {

    export class Krovak {
        // Constructor
        constructor();
    }

    export class Mercator {
        // Constructor
        constructor();
    }

    export class UTM33 {
        // Constructor
        constructor();
    }

    export class Oblique extends SMap.Projection {
        // Constructor
        constructor(id: string, config: {points: number[]; best: number; angles: number; center: number; focalLength: number; pixelSize: number; imageSize: SMap.Pixel}, orientation: number, coords: SMap.Coords);
        static create(center: SMap.Coords, orientation: number, callback: Function, errorCallback: Function): Oblique;

        // Method
        getProvider(): string;
        isValid(): boolean;
    }
}

declare module SMap.Projection.Oblique {

    export class Matrix {
        // Constructor
        constructor();
    }

    export class Triangle {
        // Constructor
        constructor();
    }

    export class Vector {
        // Constructor
        constructor();
    }
}