//http://typescript.codeplex.com/wikipage?title=Basic%20Types%20in%20TypeScript&referringTitle=Documentation

declare module BMap {

    //=====================================================
    //================= Core classes ======================
    //=====================================================

    export class Map {
        // Constructor
        constructor(container: string, opts ?: MapOptions);
        constructor(container: HTMLElement, opts ?: MapOptions);

        // Configuration
        enableDragging(): void;
        disableDragging(): void;
        enableScrollWheelZoom(): void;
        disableScrollWheelZoom(): void;
        enableDoubleClickZoom(): void;
        disableDoubleClickZoom(): void;
        enableKeyboard(): void;
        disableKeyboard(): void;
        enableInertialDragging(): void;
        disableInertialDragging(): void;
        enableContinuousZoom(): void;
        disableContinuousZoom(): void;
        enablePinchToZoom(): void;
        disablePinchToZoom(): void;
        enableAutoResize(): void;
        disableAutoResize(): void;
        setDefaultCursor(cursor: string): void;
        getDefaultCursor(): string;
        setDraggingCursor(cursor: string): void;
        getDraggingCursor(): string;
        setMinZoom(zoom: number): void;
        setMaxZoom(zoom: number): void;
        setMapStyle(mapStyle: MapStyle): void;
        disable3DBuilding(): void;

        // Map state method
        getBounds(): Bounds;
        getCenter(): Point;
        getDistance(start: Point, end: Point): number;
        getMapType(): MapType;
        getSize(): Size;
        getViewport(view: Point[], viewportOptions?: ViewportOptions): Viewport;
        getZoom(): number;

        // Edit map state method
        centerAndZoom(center: Point, zoom: number): void;
        panTo(center: Point, opts?: PanOptions): void;
        panBy(x: number, y: number, opts?: PanOptions): void;
        reset(): void;
        setCenter(center: Point): void;
        setCenter(center: string): void;
        setCurrentCity(city: string): void;
        setMapType(mapType: MapType): void;
        setViewport(view: Point[], viewportOptions?: ViewportOptions): void;
        setViewport(view: Viewport, viewportOptions?: ViewportOptions): void;
        zoomTo(zoom: number): void;
        setZoom(zoom: number): void;
        highResolutionEnabled(): void;
        zoomIn(): void;
        zoomOut(): void;
        addHotspot(hotspot: Hotspot): void;
        removeHotspot(hotspot: Hotspot): void;
        clearHotspots(): void;

        // Control methods
        addControl(control: Control): void;
        removeControl(control: Control): void;
        getContainer(): HTMLElement;

        // Right menu method
        addContextMenu(menu: ContextMenu): void;
        removeContextMenu(menu: ContextMenu): void;

        // Covering methods
        addOverlay(overlay: Overlay): void;
        removeOverlay(overlay: Overlay): void;
        clearOverlays(): void;
        openInfoWindow(infoWnd: InfoWindow, point: Point): void;
        closeInfoWindow(): void;
        pointToOverlayPixel(point: Point): void;
        overlayPixelToPoint(pixel: Pixel): Point;
        getInfoWindow(): InfoWindow;
        getOverlays(): Overlay[];
        getPanes(): MapPanes;

        // Map layer method
        addTileLayer(tileLayer: TileLayer): void;
        addTileLayer(tileLayer: CustomLayer): void;
        removeTileLayer(tilelayer: TileLayer): void;
        removeTileLayer(tilelayer: CustomLayer): void;
        getTileLayer(mapType: string): TileLayer;

        // Coordinate transformation
        pixelToPoint(pixel:Pixel): Point;
        pointToPixel(point: Point): Pixel;

        // Panoramic method
        getPanorama(): Panorama;
        setPanorama(pano: Panorama): void;

        // Events
        // click 	{type, target, point, pixel, overlay}
        // dblclick 	{type, target, pixel, point}
        // rightclick 	{type, target, point, pixel, overlay}
        // rightdblclick 	{type, target, point, pixel, overlay}
        // maptypechange 	{type, target}
        // mousemove 	{type, target, point, pixel, overlay}
        // mouseover 	{type, target}
        // mouseout 	{type, target}
        // movestart 	{type, target}
        // moving 	{type, target}
        // moveend 	{type, target}
        // zoomstart 	{type, target}
        // zoomend 	{type, target}
        // addoverlay 	{type, target}
        // addcontrol 	{type, target}
        // removecontrol 	{type, target}
        // removeoverlay 	{type, target}
        // clearoverlays 	{type, target}
        // dragstart 	{type, target, pixel, point}
        // dragging 	{type, target, pixel, point}
        // dragend 	{type, target, pixel, point}
        // addtilelayer 	{type, target}
        // removetilelayer 	{type, target}
        // load 	{type, target, pixel, point, zoom}
        // resize 	{type, target, size}
        // hotspotclick 	{type, target, spots}
        // hotspotover 	{type, target, spots}
        // hotspotout 	{type, target, spots}
        // tilesloaded 	{type, target}
        // touchstart 	{type, target, point,pixel}
        // touchmove 	{type, target, point,pixel}
        // touchend 	{type, target, point,pixel}
        // longpress 	{type, target, point,pixel}
    }

    export interface PanOptions {
        noAnimation?: boolean;
    }

    export interface MapOptions {
        minZoom?: number;
        maxZoom?: number;
        mapType?: MapType;
        enableHighResolution?: boolean;
        enableAutoResize?: boolean;
        enableMapClick?: boolean;
    }

    export class Viewport {
        // Property
        center: Point;
        zoom: number;
    }

    export interface ViewportOptions {
        enableAnimation?: boolean;
        margins?: number[];
        zoomFactor?: number;
        delay?: number;
    }

    export class MapStyle {
        // Property
        /**
         * Set on the map to show the element types, supporting point (point of interest), road (roads), water (river), land (land), building (building).
         */
        features: string[];

        /**
         * Set base map style, now supports normal (default style), dark (dark style), light (light style) three.
         */
        style: string;
    }


    //=====================================================
    //================= Foundation Classes ================
    //=====================================================

    export class Point {
        // Constructor
        constructor(lng: number, lat: number);

        // Property
        lng: number;
        lat: number;

        // Method
        equals(other: Point): boolean;
    }

    export class Pixel {
        // Constructor
        constructor(x: number, y: number);

        // Property
        x: number;
        y: number;

        // Method
        equals(other: Pixel): boolean;
    }

    export class Bounds {
        // Constructor
        constructor(sw: Point, ne: Point);

        // Method
        equals(other: Bounds): boolean;
        containsPoint(point: Point): boolean;
        containsBounds(bounds: Bounds): boolean;
        intersects(other: Bounds): Bounds;
        extend(point: Point): void;
        getCenter(): Point;
        isEmpty(): boolean;
        getSouthWest(): Point;
        getNorthEast(): Point;
        toSpan(): Point;
    }

    export class Size {
        // Constructor
        constructor(width: number, height: number);

        // Property
        width: number;
        height: number;

        // Method
        equals(other: Size): boolean;
    }


    //=====================================================
    //================= Control class =====================
    //=====================================================

    export class Control {
        // Constructor
        constructor();

        // Property
        defaultAnchor: ControlAnchor;
        defaultOffset: Size;

        // Method
        initialize(map: Map): HTMLElement;
        setAnchor(anchor: ControlAnchor): void;
        getAnchor(): ControlAnchor;
        setOffset(offset: Size): void;
        getOffset(): Size;
        show(): void;
        hide(): void;
        isVisible(): boolean;
    }

    export interface NavigationControlOptions {
        anchor?: ControlAnchor;
        offset?: Size;
        type?: NavigationControlType;
        showZoomInfo?: boolean;
        enableGeolocation?: boolean;
    }

    export interface ScaleControlOptions {
        anchor?: ControlAnchor;
        offset?: Size;
    }

    export interface CopyrightControlOptions {
        anchor?: ControlAnchor;
        offset?: Size;
    }

    export enum ControlAnchor {
        BMAP_ANCHOR_TOP_LEFT,
        BMAP_ANCHOR_TOP_RIGHT,
        BMAP_ANCHOR_BOTTOM_LEFT,
        BMAP_ANCHOR_BOTTOM_RIGHT
    }

    export class OverviewMapControl extends Control {
        // Constructor
        constructor(opts?: OverviewMapControlOptions);

        // Method
        changeView(): void;
        setSize(size: Size): void;
        getSize(): Size;

        // Events
        // viewchanged 	event{type, target, isOpen}
        // viewchanging 	event{type, target}
    }

    export enum LengthUnit {
        BMAP_UNIT_METRIC,
        BMAP_UNIT_IMPERIAL
    }

    export class MapTypeControl extends Control {
        // Constructor
        constructor(opts?: MapTypeControlOptions);
    }

    export class NavigationControl extends Control {
        // Constructor
        constructor(type?: NavigationControlOptions);

        // Method
        getType(): NavigationControlType;
        setType(type: NavigationControlType): void;
    }

    export interface OverviewMapControlOptions {
        anchor?: ControlAnchor;
        offset?: Size;
        size?: Size;
        isOpen?: boolean;
    }

    export class CopyrightControl extends Control {
        // Constructor
        constructor(opts?: CopyrightControlOptions);

        // Method
        addCopyright(copyright: Copyright): void;
        removeCopyright(id: number): void;
        getCopyright(id: number): Copyright;
        getCopyrightCollection(): Copyright[];
    }

    export interface MapTypeControlOptions {
        anchor?: ControlAnchor;
        type?: MapTypeControlType;
        mapTypes?: MapType[];
    }

    export enum NavigationControlType {
        BMAP_NAVIGATION_CONTROL_LARGE,
        BMAP_NAVIGATION_CONTROL_SMALL,
        BMAP_NAVIGATION_CONTROL_PAN,
        BMAP_NAVIGATION_CONTROL_ZOOM
    }

    export class ScaleControl extends Control {
        // Constructor
        constructor(opts?: ScaleControlOptions);

        // Method
        getUnit(): LengthUnit;
        setUnit(unit: LengthUnit): void;
    }

    export class Copyright {
        // Property
        id: number;
        content: string;
        bounds: Bounds;
    }

    export enum MapTypeControlType {
        BMAP_MAPTYPE_CONTROL_HORIZONTAL,
        BMAP_MAPTYPE_CONTROL_DROPDOWN,
        BMAP_MAPTYPE_CONTROL_MAP
    }

    export class GeolocationControl extends Control {
        // Constructor
        constructor();

        // Method
        location(): void;
        getAddressComponent(): AddressComponent;

        // Events
        // locationSuccess 	{point, AddressComponent}
        // locationError 	{StatusCode}
    }

    export interface GeolocationControlOptions {
        anchor?: ControlAnchor;
        offset?: Size;
        showAddressBar?: boolean;
        enableAutoLocation?: boolean;
        locationIcon?: Icon;
    }

    export enum StatusCode {
        BMAP_STATUS_SUCCESS,
        BMAP_STATUS_CITY_LIST,
        BMAP_STATUS_UNKNOWN_LOCATION,
        BMAP_STATUS_UNKNOWN_ROUTE,
        BMAP_STATUS_INVALID_KEY,
        BMAP_STATUS_INVALID_REQUEST,
        BMAP_STATUS_PERMISSION_DENIED,
        BMAP_STATUS_SERVICE_UNAVAILABLE,
        BMAP_STATUS_TIMEOUT
    }

    export class PanoramaControl extends Control {
        // Constructor
        constructor();
    }


    //=====================================================
    //================= Cover class =======================
    //=====================================================

    export class Overlay {
        // Method
        initialize(map: Map): HTMLElement;
        isVisible(): boolean;

        /**
         * Abstract method, when the map changes state by the system call to overlay paint. Custom coverings need to implement this method.
         *
         (From 1.1 new)
         */
        dispose(): void;

        draw(): void;
        show(): void;
        hide(): void;
    }

    export enum SymbolShapeType {
        BMap_Symbol_SHAPE_CIRCLE,
        BMap_Symbol_SHAPE_RECTANGLE,
        BMap_Symbol_SHAPE_RHOMBUS,
        BMap_Symbol_SHAPE_STAR,
        BMap_Symbol_SHAPE_BACKWARD_CLOSED_ARROW,
        BMap_Symbol_SHAPE_FORWARD_CLOSED_ARROW,
        BMap_Symbol_SHAPE_BACKWARD_OPEN_ARROW,
        BMap_Symbol_SHAPE_FORWARD_OPEN_ARROW,
        BMap_Symbol_SHAPE_POINT,
        BMap_Symbol_SHAPE_PLANE,
        BMap_Symbol_SHAPE_CAMERA,
        BMap_Symbol_SHAPE_WARNING,
        BMap_Symbol_SHAPE_SMILE,
        BMap_Symbol_SHAPE_CLOCK
    }

    export interface PolylineOptions {
        strokeColor?: string;
        strokeWeight?: number;
        strokeOpacity?: number;
        strokeStyle?: string;
        enableMassClear?: boolean;
        enableEditing?: boolean;
        enableClicking?: boolean;
    }

    export interface GroundOverlayOptions {
        opacity?: number;
        imageURL?: string;
        displayOnMinLevel?: number;
        displayOnMaxLevel?: number;
    }

    export class Marker extends Overlay {
        // Constructor
        constructor(point:Point, opts?: MarkerOptions);

        // Method
        openInfoWindow(infoWnd: InfoWindow): void;
        closeInfoWindow(): void;
        setIcon(icon: Icon): void;
        getIcon(): Icon;
        setPosition(position: Point): void;
        getPosition(): Point;
        setOffset(offset: Size): void;
        getOffset(): Size;
        getLabel(): Label;
        setLabel(label: Label): void;
        setTitle(title: string): void;
        getTitle(): string;
        setTop(isTop: boolean): void;
        enableDragging(): void;
        disableDragging(): void;
        enableMassClear(): void;
        disableMassClear(): void;
        setZIndex(zIndex: number): void;
        getMap(): Map;
        addContextMenu(menu: ContextMenu): void;
        removeContextMenu(menu: ContextMenu): void;
        setAnimation(animation: Animation): void;
        getRotation(): number;
        setShadow(shadow: Icon): void;
        getShadow(): Icon;
        addEventListener(event: string, handler: Function): void;
        removeEventListener(event: string, handler: Function): void;


        // Events
        // click 	event{type, target}
        // dblclick 	event{type, target, point,pixel}
        // mousedown 	event{type, target, point,pixel}
        // mouseup 	event{type, target, point,pixel}
        // mouseout 	event{type, target, point,pixel}
        // mouseover 	event{type, target, point,pixel}
        // remove 	event{type, target}
        // infowindowclose 	event{type, target}
        // infowindowopen 	event{type, target}
        // dragstart 	event{type, target}
        // dragging 	event{type, target, pixel, point}
        // dragend 	event{type, target, pixel, point}
        // rightclick 	event{type, target}
    }

    export interface SymbolOptions {
        anchor?: Size;
        fillColor?: string;
        fillOpacity?; number;
        scale?: number;
        rotation?: number;
        strokeColor?; string;
        strokeOpacity?: number;
        strokeWeight?: number;
    }

    export class IconSequence {
        // Constructor
        constructor(symbol: Symbol, offset?: string, repeat?: string, fixedRotation?: boolean);
    }

    export class PointCollection {
        // Constructor
        constructor(points: Point[], opts?: PointCollectionOption);

        // Method
        setPoints(points: Point[]): void;
        setStyles(styles: PointCollectionOption): void;
        clear(): void;

        // Events
        // click 	event{type, target,point}
        // mouseover 	event{type, target,point}
        // mouseout 	event{type, target,point}
    }

    export interface MarkerOptions {
        offset?: Size;
        icon?: Icon;
        enableMassClear?: boolean;
        enableDragging?: boolean;
        enableClicking?: boolean;
        raiseOnDrag?: boolean;
        draggingCursor?: string;
        rotation?: number;
        shadow?: Icon;
        title?: string;
    }

    export class InfoWindow {
        // Constructor
        constructor(content: string, opts?: InfoWindowOptions);
        constructor(content: HTMLElement, opts?: InfoWindowOptions);

        // Method
        setWidth(width: number): void;
        setHeight(height: number): void;
        redraw(): void;
        setTitle(title: string): void;
        setTitle(title: HTMLElement): void;
        getTitle(): string;
        getTitle(): HTMLElement;
        setContent(content: string): void;
        setContent(content: HTMLElement): void;
        getContent(): string;
        getContent(): HTMLElement;
        getPosition(): Point;
        enableMaximize(): void;
        disableMaximize(): void;
        isOpen(): boolean;
        setMaxContent(content: string): void;
        maximize(): void;
        restore(): void;
        enableAutoPan(): void;
        disableAutoPan(): void;
        enableCloseOnClick(): void;
        disableCloseOnClick(): void;
        addEventListener(event: string, handler: Function): void;
        removeEventListener(event: string, handler: Function): void;

        // Events
        // close 	event{type, target, point}
        // event{type, target, point}
        // maximize 	event{type, target}
        // restore 	event{type, target}
        // clickclose 	event{type, target}
    }

    export class Polygon {
        // Constructor
        constructor(points: Point[], opts?: PolygonOptions);

        // Method
        setPath(path: Point[]): void;
        getPath(): Point[];
        setStrokeColor(color: string): void;
        getStrokeColor(): string;
        setFillColor(color: string): void;
        getFillColor(): string;
        setStrokeOpacity(opacity: number): void;
        getStrokeOpacity(): number;
        setFillOpacity(opacity: number): void;
        getFillOpacity(): number;
        setStrokeWeight(weight: number): void;
        getStrokeWeight(): number;
        setStrokeStyle(style: string): void;
        getStrokeStyle(): string;
        getBounds(): Bounds;
        enableEditing(): void;
        disableEditing(): void;
        enableMassClear(): void;
        disableMassClear(): void;
        setPositionAt(index: number, point: Point): void;
        addEventListener(event: string, handler: Function): void;
        removeEventListener(event: string, handler: Function): void;
        getMap(): Map;

        // Events
        // click 	event{type, target, point, pixel}
        // dblclick 	event{type, target, point, pixel}
        // mousedown 	event{type, target, point, pixel}
        // mouseup 	event{type, target, point, pixel}
        // mouseout 	event{type, target, point, pixel}
        // mouseover 	event{type, target, point, pixel}
        // remove 	event{type, target}
        // lineupdate 	event{type, target}
    }

    export interface PointCollectionOption {
        shape?: ShapeType;
        color?: string;
        size?: SizeType;
    }

    export enum Animation {
        BMAP_ANIMATION_DROP,
        BMAP_ANIMATION_BOUNCE
    }

    export interface InfoWindowOptions {
        width?: number;
        height?: number;
        maxWidth?: number;
        offset?: Size;
        title?: string;
        enableAutoPan?: boolean;
        enableCloseOnClick?: boolean;
        enableMessage?: boolean;
        message?: string;
    }

    export interface PolygonOptions {
        strokeColor?: string;
        fillColor?: string;
        strokeWeight?: number;
        strokeOpacity?: number;
        fillOpacity?: number;
        strokeStyle?: string;
        enableMassClear?: boolean;
        enableEditing?: boolean;
        enableClicking?: boolean;
    }

    export enum ShapeType {
        BMAP_POINT_SHAPE_CIRCLE,
        BMAP_POINT_SHAPE_STAR,
        BMAP_POINT_SHAPE_SQUARE,
        BMAP_POINT_SHAPE_RHOMBUS,
        BMAP_POINT_SHAPE_WATERDROP
    }

    export class Icon {
        // Constructor
        constructor(points: Point[], opts?: IconOptions);

        // Property
        anchor: Size;
        size: Size;
        imageOffset: Size;
        imageSize: Size;
        imageUrl: string;
        infoWindowAnchor: Size;
        printImageUrl: string;

        // Method
        setImageUrl(imageUrl: string): void;
        setSize(size: Size): void;
        setImageSize(offset: Size): void;
        setAnchor(anchor: Size): void;
        setImageOffset(offset: Size): void;
        setInfoWindowAnchor(anchor: Size): void;
        setPrintImageUrl(url: string): void;
    }

    export class Label {
        // Constructor
        constructor(content: string, opts?: LabelOptions);

        // Method
        setStyle(styles: Object): void;
        setContent(content: string): void;
        setPosition(position: Point): void;
        getPosition(): Point;
        setOffset(offset: Size): void;
        getOffset(): Size;
        setTitle(title: string): void;
        getTitle(): string;
        enableMassClear(): void;
        disableMassClear(): void;
        setZIndex(zIndex: number): void;
        getMap(): Map;
        addEventListener(event: string, handler: Function): void;
        removeEventListener(event: string, handler: Function): void;


        // Events
        // click 	event{type, target}
        // dblclick 	event{type, target}
        // mousedown 	event{type, target}
        // mouseup 	event{type, target}
        // mouseout 	event{type, target}
        // mouseover 	event{type, target}
        // remove 	event{type, target}
        // rightclick 	event{type, target}
    }

    export class Circle {
        // Constructor
        constructor(center: Point, radius: number, opts?: CircleOptions);

        // Method
        setCenter(center: Point): void;
        getCenter(): Point;
        setRadius(radius: number): void;
        getRadius(): number;
        getBounds(): Bounds;
        setStrokeColor(color: string): void;
        getStrokeColor(): string;
        setFillColor(color: string): void;
        getFillColor(): string;
        setStrokeOpacity(opacity: number): void;
        getStrokeOpacity(): number;
        setFillOpacity(opacity: number): void;
        getFillOpacity(): number;
        setStrokeWeight(weight: number): void;
        getStrokeWeight(): number;
        setStrokeStyle(style: string): void;
        getStrokeStyle(): string;
        enableEditing(): void;
        disableEditing(): void;
        enableMassClear(): void;
        disableMassClear(): void;
        getMap(): Map;
        addEventListener(event: string, handler: Function): void;
        removeEventListener(event: string, handler: Function): void;

        // Events
        // click 	event{type, target, point, pixel}
        // dblclick 	event{type, target, point, pixel}
        // mousedown 	event{type, target, point, pixel}
        // mouseup 	event{type, target, point, pixel}
        // mouseout 	event{type, target, point, pixel}
        // mouseover 	event{type, target, point, pixel}
        // remove 	event{type, target}
        // lineupdate 	event{type, target}
    }

    export enum SizeType {
        BMAP_POINT_SIZE_TINY,
        BMAP_POINT_SIZE_SMALLER,
        BMAP_POINT_SIZE_SMALL,
        BMAP_POINT_SIZE_NORMAL,
        BMAP_POINT_SIZE_BIG,
        BMAP_POINT_SIZE_BIGGER,
        BMAP_POINT_SIZE_HUGE
    }

    export interface IconOptions {
        anchor?: Size;
        imageOffset?: Size;
        infoWindowAnchor?: Size;
        printImageUrl?: string;
    }

    export interface LabelOptions {
        offset?: Size;
        position?: Point;
        enableMassClear?: boolean;
    }

    export interface CircleOptions {
        strokeColor?: string;
        fillColor?: string;
        strokeWeight?: number;
        strokeOpacity?: number;
        fillOpacity?: number;
        strokeStyle?: string;
        enableMassClear?: boolean;
        enableEditing?: boolean;
        enableClicking?: boolean;
    }

    export class Hotspot {
        // Constructor
        constructor(position: Point, options?: HotspotOptions);

        // Method
        getPosition(): Point;
        setPosition(position: Point): void;
        getText(): string;
        setText(text: string): void;
        getUserData(): any;
        setUserData(data: any): void;
    }

    export class Symbol {
        // Constructor
        constructor(path: string, opts?: SymbolOptions);
        constructor(path: SymbolShapeType, opts?: SymbolOptions);

        // Method
        setPath(path: string): void;
        setPath(path: SymbolShapeType): void;
        setAnchor(anchor: Size): void;
        setRotation(rotation: number): void;
        setScale(scale: number): void;
        setStrokeWeight(strokeWeight: number): void;
        setStrokeColor(color: string): void;
        setStrokeOpacity(opacity: number): void;
        setFillOpacity(opacity: number): void;
        setFillColor(color: string): void;
    }

    export class Polyline {
        // Constructor
        constructor(points: Point[], opts?: PolylineOptions);

        // Method
        setPath(path: Point[]): void;
        getPath(): Point[];
        setStrokeColor(color: string): void;
        getStrokeColor(): string;
        setStrokeOpacity(opacity: number): void;
        getStrokeOpacity(): number;
        setStrokeWeight(weight: number): void;
        getStrokeWeight(): number;
        setStrokeStyle(style: string): void;
        getStrokeStyle(): string;
        getBounds(): Bounds;
        enableEditing(): void;
        disableEditing(): void;
        enableMassClear(): void;
        disableMassClear(): void;
        setPositionAt(index: number, point: Point): void;
        getMap(): Map;
        addEventListener(event: string, handler: Function): void;
        removeEventListener(event: string, handler: Function): void;

        // Events
        // click 	event{type, target, point, pixel}
        // dblclick 	event{type, target, point, pixel}
        // mousedown 	event{type, target, point, pixel}
        // mouseup 	event{type, target, point, pixel}
        // mouseout 	event{type, target, point, pixel}
        // mouseover 	event{type, target, point, pixel}
        // remove 	event{type, target}
        // lineupdate 	event{type, target}
    }

    export class GroundOverlay {
        // Constructor
        constructor(bounds: Bounds, opts?: GroundOverlayOptions);

        // Method
        setBounds(bounds: Bounds): void;
        getBounds(): Bounds;
        setOpacity(opacity: number): void;
        getOpacity(): number;
        setImageURL(url: string): void;
        getImageURL(): string;
        setDisplayOnMinLevel(level: number): void;
        getDisplayOnMinLevel(): number;
        setDispalyOnMaxLevel(level: number): void;
        getDispalyOnMaxLevel(): number;

        // Events
        // click 	event{type, target}
        // dblclick 	event{type, target}
    }

    export interface HotspotOptions {
        text?: string;
        offsets?: number[];
        userData?: any;
        minZoom?: number;
        maxZoom?: number;
    }

    export class MapPanes {
        floatPane: HTMLElement;
        markerMouseTarget: HTMLElement;
        floatShadow: HTMLElement;
        labelPane: HTMLElement;
        markerPane: HTMLElement;
        markerShadow: HTMLElement;
        mapPane: HTMLElement;
    }


    //=====================================================
    //================= Tools =============================
    //=====================================================



    //=====================================================
    //================= Right menu classes ================
    //=====================================================

    export class ContextMenu {
        // Constructor
        constructor();

        // Method
        addItem(item: MenuItem): void;
        getItem(index: number): MenuItem;
        removeItem(item: MenuItem): void;
        addSeparator(): void;
        removeSeparator(index: number): void;

        // Events
        // open 	event{type, target, point, pixel}
        // close 	event{type, target, point, pixel}
    }

    export class MenuItem {
        // Constructor
        constructor(text: string, callback: Function, opts?: MenuItemOptions);

        // Method
        setText(text: string): void;
        setIcon(iconUrl: string): void;
        enable(): void;
        disable(): void;
    }

    export interface MenuItemOptions {
        width?: number;
        id?: string;
        //iconUrl?: string,ContextMenuIcon;
        iconUrl?: string;
        iconUrl?: ContextMenuIcon;
    }

    export enum ContextMenuIcon {
        BMAP_CONTEXT_MENU_ICON_ZOOMIN,
        BMAP_CONTEXT_MENU_ICON_ZOOMOUT
    }


    //=====================================================
    //================= Map type classes ==================
    //=====================================================

    export class MapType {
        // Constructor
        constructor(name: string, layers: TileLayer, options?: MapTypeOptions);
        constructor(name: string, layers: TileLayer[], options?: MapTypeOptions);

        // Method
        getName(): string;
        getTileLayer(): TileLayer;
        getMinZoom(): number;
        getMaxZoom(): number;
        getProjection(): Projection;
        getTextColor(): string;
        getTips(): string;
    }

    export interface MapTypeOptions {
        minZoom?: number;
        maxZoom?:number;
        errorImageUrl?: string;
        textColor?: number;
        tips?: string;
    }

    export class Projection {
        lngLatToPoint(lngLat: Point): Pixel;
        pointToLngLat(point:Pixel): Point;
    }

    export class MercatorProjection extends Projection {
    }

    export class PerspectiveProjection extends Projection {
    }


    //=====================================================
    //================= Map layer category ================
    //=====================================================

    export class TileLayer {
        // Constructor
        constructor(opts?: TileLayerOptions);

        // Method
        getTilesUrl(tileCoord: Pixel, zoom: number): string;
        getMapType(): string;
        getMapType(): MapType;
        getCopyright(): number;
        isTransparentPng(): number;
    }

    export interface TileLayerOptions {
        transparentPng?: boolean;
        tileUrlTemplate?: string;
        copyright?: Copyright;
        zIndex?: number;
    }

    export class TrafficLayer {
        // Constructor
        constructor(opts?: TrafficLayerOptions);
    }

    export interface TrafficLayerOptions {
        predictDate?: PredictDate;
    }

    export class PredictDate {
        weekday: number;
        hour: number;
    }

    export class CustomLayer {
        // Constructor
        constructor(opts?: CustomLayerOptions);

        // Events
        // hotspotclick 	event{type, target, content}
    }

    export interface CustomLayerOptions {
        databoxId?: string;
        geotableId?: string;
        q?: string;
        tags?: string;
        filter?: string;
        pointDensityType?: PointDensityType;
    }

    export enum PointDensityType {
        BMAP_POINT_DENSITY_HIGH,
        BMAP_POINT_DENSITY_MEDIUM,
        BMAP_POINT_DENSITY_LOW
    }


    //=====================================================
    //================= Service category ==================
    //=====================================================

    export class LocalSearch {
        // Constructor
        constructor(location: Map, opts?: LocalSearchOptions);
        constructor(location: Point, opts?: LocalSearchOptions);
        constructor(location: string, opts?: LocalSearchOptions);

        // Method
        search(keyword: string, option?: Object): void;
        search(keyword: string[], option?: Object): void;
        searchInBounds(keyword: string, bounds: Bounds, option?: Object):void;
        searchInBounds(keyword: string[], bounds: Bounds, option?: Object):void;
        searchNearby(keyword: string, center: LocalResultPoi, radius: number, option?:Object): void;
        searchNearby(keyword: string, center: string, radius: number, option?:Object): void;
        searchNearby(keyword: string, center: Point, radius: number, option?:Object): void;
        searchNearby(keyword: string[], center: LocalResultPoi, radius: number, option?:Object): void;
        searchNearby(keyword: string[], center: string, radius: number, option?:Object): void;
        searchNearby(keyword: string[], center: Point, radius: number, option?:Object): void;
        getResults(): LocalResult;
        getResults(): LocalResult[];
        clearResults(): void;
        gotoPage(page: number): void;
        enableAutoViewport(): void;
        disableAutoViewport(): void;
        enableFirstResultSelection(): void;
        disableFirstResultSelection(): void;
        setLocation(location: Map): void;
        setLocation(location: Point): void;
        setLocation(location: string): void;
        setPageCapacity(capacity: number): void;
        getPageCapacity(): number;
        setSearchCompleteCallback(callback: Function): void;
        setMarkersSetCallback(callback: Function): void;
        setInfoHtmlSetCallback(callback: Function): void;
        setResultsHtmlSetCallback(callback: Function): void;
        getStatus(): StatusCode;
    }

    export interface LocalSearchOptions {
        renderOptions?: LocalRenderOptions;
        onMarkersSet?: Function;
        onInfoHtmlSet?: Function;
        onResultsHtmlSet?: Function;
        pageCapacity?: number;
        onSearchComplete?: Function;
    }

    export class CustomData {
        // Property
        geotableId: number;
        tags: string;
        filter: string;
    }

    export interface RenderOptions {
        map?: Map;
        panel?: string;
        panel?: HTMLElement;
        //panel?: string,HTMLElement;
        selectFirstResult?: boolean;
        autoViewport?: boolean;
        highlightMode?: HighlightModes;
    }

    export interface LocalRenderOptions extends RenderOptions {

    }

    export class LocalResult {
        // Method
        getPoi(i: number): LocalResultPoi;
        getCurrentNumPois(): number;
        getNumPois(): number;
        getNumPages(): number;
        getPageIndex(): number;
        getCityList(): Object[];

        // Property
        keyword: string;
        center: LocalResultPoi;
        radius: number;
        bounds: Bounds;
        city: string;
        moreResultsUrl: string;
        province: string;
        suggestions: string;
    }

    export class LocalResultPoi {
        // Property
        title: string;
        point: Point;
        url: string;
        address: string;
        city: string;
        phoneNumber: string;
        postcode: string;
        type: PoiType;
        isAccurate: boolean;
        province: string;
        tags: string[];
        detailUrl: string;
    }

    export enum PoiType {
        BMAP_POI_TYPE_NORMAL,
        BMAP_POI_TYPE_BUSSTOP,
        BMAP_POI_TYPE_SUBSTOP
    }

    export class TransitRoute {
        // Constructor
        constructor(location: Map, opts?: TransitRouteOptions);
        constructor(location: Point, opts?: TransitRouteOptions);
        constructor(location: string, opts?: TransitRouteOptions);

        // Method
        search(start: string, end: string): void;
        search(start: string, end: Point): void;
        search(start: string, end: LocalResultPoi): void;
        search(start: Point, end: string): void;
        search(start: Point, end: Point): void;
        search(start: Point, end: LocalResultPoi): void;
        search(start: LocalResultPoi, end: string): void;
        search(start: LocalResultPoi, end: Point): void;
        search(start: LocalResultPoi, end: LocalResultPoi): void;
        getResults(): TransitRouteResult;
        clearResults(): void;
        enableAutoViewport(): void;
        disableAutoViewport(): void;
        setPageCapacity(capacity: number): void;
        setLocation(location: Map): void;
        setLocation(location: Point): void;
        setLocation(location: string): void;
        setPolicy(policy: TransitPolicy): void;
        setSearchCompleteCallback(callback: Function): void;
        setMarkersSetCallback(callback: Function): void;
        setInfoHtmlSetCallback(callback: Function): void;
        setPolylinesSetCallback(callback: Function): void;
        setResultsHtmlSetCallback(callback: Function): void;
        getStatus(): StatusCode;
        toString(): string;
    }

    export interface TransitRouteOptions {
        renderOptions ?: RenderOptions;
        policy?: TransitPolicy;
        pageCapacity?: number;
        onSearchComplete?: Function;
        onMarkersSet?: Function;
        onInfoHtmlSet?: Function;
        onPolylinesSet?: Function;
        onResultsHtmlSet?: Function;
    }

    export enum TransitPolicy {
        BMAP_TRANSIT_POLICY_LEAST_TIME,
        BMAP_TRANSIT_POLICY_LEAST_TRANSFER,
        BMAP_TRANSIT_POLICY_LEAST_WALKING,
        BMAP_TRANSIT_POLICY_AVOID_SUBWAYS
    }

    export class TransitRouteResult {
        // Method
        getStart(): LocalResultPoi;
        getEnd(): LocalResultPoi;
        getNumPlans(): number;
        getPlan(i: number): TransitRoutePlan;

        // Property
        policy: TransitPolicy;
        city: string;
        moreResultsUrl: string;
    }

    export class TransitRoutePlan {
        // Method
        getNumLines(): number;
        getLine(i: number): Line;
        getNumRoutes(): number;
        getRoute(i: number): Route;
        getDistance(format?: boolean): string;
        getDistance(format?: boolean): number;
        getDuration(format: boolean): string;
        getDuration(format: boolean): number;
        getDescription(includeHtml?: boolean): string;
    }

    export interface WalkingRouteOptions {
        renderOptions?: RenderOptions;
        onSearchComplete?: Function;
        onMarkersSet?: Function;
        onPolylinesSet?: Function;
        onInfoHtmlSet?: Function;
        onResultsHtmlSet?: Function;
    }

    export enum LineType {
        BMAP_LINE_TYPE_BUS,
        BMAP_LINE_TYPE_SUBWAY,
        BMAP_LINE_TYPE_FERRY
    }

    export class DrivingRoute {
        // Constructor
        constructor(location: Map, opts?: DrivingRouteOptions);
        constructor(location: Point, opts?: DrivingRouteOptions);
        constructor(location: string, opts?: DrivingRouteOptions);

        // Method
        search(start: string, end: string, options?: Object): void;
        search(start: string, end: Point, options?: Object): void;
        search(start: string, end: LocalResultPoi, options: Object): void;
        search(start: Point, end: string, options?: Object): void;
        search(start: Point, end: Point, options?: Object): void;
        search(start: Point, end: LocalResultPoi, options: Object): void;
        search(start: LocalResultPoi, end: string, options?: Object): void;
        search(start: LocalResultPoi, end: Point, options?: Object): void;
        search(start: LocalResultPoi, end: LocalResultPoi, options: Object): void;
        getResults(): DrivingRouteResult;
        clearResults(): void;
        enableAutoViewport(): void;
        disableAutoViewport(): void;
        setLocation(location: Map);
        setLocation(location: Point);
        setLocation(location: string);
        setPolicy(policy: DrivingPolicy): void;
        setSearchCompleteCallback(callback: Function): void;
        setMarkersSetCallback(callback: Function): void;
        setInfoHtmlSetCallback(callback: Function): void;
        setPolylinesSetCallback(callback: Function): void;
        setResultsHtmlSetCallback(callback: Function): void;
        getStatus(): StatusCode;
        toString(): string;
    }

    export interface DrivingRouteOptions {
        renderOptions?: RenderOptions;
        policy?: DrivingPolicy;
        onSearchComplete?: Function;
        onMarkersSet?: Function;
        onInfoHtmlSet?: Function;
        onPolylinesSet?: Function;
        onResultsHtmlSet?: Function;
    }

    export enum DrivingPolicy {
        BMAP_DRIVING_POLICY_LEAST_TIME,
        BMAP_DRIVING_POLICY_LEAST_DISTANCE,
        BMAP_DRIVING_POLICY_AVOID_HIGHWAYS
    }

    export class DrivingRouteResult {
        // Method
        getStart(): LocalResultPoi;
        getEnd(): LocalResultPoi;
        getNumPlans(): number;
        getPlan(i: number): RoutePlan;

        // Property
        policy: DrivingPolicy;
        city: string;
        moreResultsUrl: string;
        taxiFare: TaxiFare;
    }

    export class TaxiFare {
        // Property
        day: TaxiFareDetail;
        night: TaxiFareDetail;
        distance: number;
        remark: string;
    }

    export class TaxiFareDetail {
        // Property
        initialFare: number;
        unitFare: number;
        totalFare: number;
    }

    export class RoutePlan {
        // Method
        getNumRoutes(): number;
        getRoute(i: number): Route;
        getDistance(format?: boolean): string;
        getDistance(format?: boolean): number;
        getDuration(format?: boolean): string;
        getDuration(format?: boolean): number;
        getDragPois(): LocalResultPoi[];
    }

    export class Route {
        // Constructor
        constructor();

        // Method
        getNumSteps(): number;
        getStep(i: number): Step;
        getDistance(format: boolean): string;
        getDistance(format: boolean): number;
        getIndex(): number;
        getPolyline(): Polyline;
        getPoints(): Point[];
        getPath(): Point[];
        getRouteType(): RouteType;
    }

    export enum RouteType {
        BMAP_ROUTE_TYPE_DRIVING,
        BMAP_ROUTE_TYPE_WALKING
    }

    export class Step {
        // Method
        getPosition(): Point;
        getIndex(): number;
        getDescription(includeHtml: boolean): string;
        getDistance(format: boolean): string;
        getDistance(format: boolean): number;
    }

    export class WalkingRoute {
        // Constructor
        constructor(location: Map, opts?: WalkingRouteOptions);
        constructor(location: Point, opts?: WalkingRouteOptions);
        constructor(location: string, opts?: WalkingRouteOptions);

        // Method
        search(start: string, end: string): void;
        search(start: string, end: Point): void;
        search(start: string, end: LocalResultPoi): void;
        search(start: Point, end: string): void;
        search(start: Point, end: Point): void;
        search(start: Point, end: LocalResultPoi): void;
        search(start: LocalResultPoi, end: string): void;
        search(start: LocalResultPoi, end: Point): void;
        search(start: LocalResultPoi, end: LocalResultPoi): void;
        getResults(): WalkingRouteResult;
        clearResults(): void;
        enableAutoViewport(): void;
        disableAutoViewport(): void;
        setLocation(location: Map): void;
        setLocation(location: Point): void;
        setLocation(location: string): void;
        setSearchCompleteCallback(callback: Function): void;
        setMarkersSetCallback(callback: Function);
        setInfoHtmlSetCallback(callback: Function): void;
        setPolylinesSetCallback(callback: Function): void;
        setResultsHtmlSetCallback(callback: Function): void;
        getStatus(): StatusCode;
        toString(): string;
    }

    export enum HighlightModes {
        BMAP_HIGHLIGHT_STEP,
        BMAP_HIGHLIGHT_ROUTE
    }

    export class WalkingRouteResult {
        // Method
        getStart(): LocalResultPoi;
        getEnd(): LocalResultPoi;
        getNumPlans(): number;
        getPlan(i: number): RoutePlan;

        // Property
        city: string;
    }

    export class Geocoder {
        // Constructor
        constructor();

        // Method
        getPoint(address: string, callback: Function, city: string): void;
        getLocation(point: Point, callback: Function, options?: LocationOptions): void;
    }

    export class GeocoderResult {
        // Property
        point: Point;
        address: string;
        addressComponents: AddressComponent;
        surroundingPois: LocalResultPoi[];
        business: string;
    }

    export class AddressComponent {
        // Property
        streetNumber: string;
        street: string;
        district: string;
        city: string;
        province: string;
    }

    export interface LocationOptions {
        poiRadius?: number;
        numPois?: number;
    }

    export class LocalCity {
        // Constructor
        constructor(opts?: LocalCityOptions);

        // Method
        get(callback: Function): void;
    }

    export interface LocalCityOptions {
        renderOptions?: RenderOptions;
    }

    export class LocalCityResult {
        // Property
        center: Point;
        level: number;
        name: string;
    }

    export class TrafficControl extends Control {
        // Constructor
        constructor();

        // Method
        setPanelOffset(offset: Size): void;
        show(): void;
        hide(): void;
    }

    export class Geolocation {
        // Constructor
        constructor();

        // Method
        getCurrentPosition(callback: Function, options?: PositionOptions): void;
        getStatus(): StatusCode;
    }

    export class GeolocationResult {
        // Property
        point: Point;
        accuracy: number;
    }

    export interface PositionOptions {
        enableHighAccuracy?: boolean;
        timeout?: number;
        maximumAge?: number;
    }

    export class BusLineSearch {
        // Constructor
        constructor(location: Map, options?: BusLineSearchOptions);
        constructor(location: Point, options?: BusLineSearchOptions);
        constructor(location: string, options?: BusLineSearchOptions);

        // Method
        getBusList(keyword: string): void;
        getBusLine(busListItem: BusListItem): void;
        clearResults(): void;
        enableAutoViewport(): void;
        disableAutoViewport(): void;
        setLocation(location: Map): void;
        setLocation(location: Point): void;
        setLocation(location: string): void;
        getStatus(): StatusCode;
        toString(): string;
        setGetBusListCompleteCallback(callback: Function): void;
        setGetBusLineCompleteCallback(callback: Function): void;
        setBusListHtmlSetCallback(callback: Function): void;
        setBusLineHtmlSetCallback(callback: Function): void;
        setPolylinesSetCallback(callback: Function): void;
        setMarkersSetCallback(callback: Function): void;
    }

    export interface BusLineSearchOptions {
        renderOptions?: RenderOptions;
        onGetBusListComplete?: Function;
        onGetBusLineComplete?: Function;
        onBusListHtmlSet?: Function;
        onBusLineHtmlSet?: Function;
        onPolylinesSet?: Function;
        onMarkersSet?: Function;
    }

    export class BusListResult {
        // Method
        getNumBusList(): number;
        getBusListItem(i: number): BusListItem;

        // Property
        keyword: string;
        city: string;
        moreResultsUrl: string;
    }

    export class BusLine {
        // Method
        getNumBusStations(): number;
        getBusStation(i: number): BusStation;
        getPath(): Point[];
        getPolyline(): Polyline;

        // Property
        name: string;
        startTime: string;
        endTime: string;
        company: string;
    }

    export class BusListItem {
        // Property
        name: string;
    }

    export class BusStation {
        // Property
        name: string;
        position: Point;
    }

    export class Autocomplete {
        // Constructor
        constructor(options?: AutocompleteOptions);

        // Method
        show(): void;
        hide(): void;
        setTypes(types: string[]): void;
        setLocation(location: string): void;
        setLocation(location: Map): void;
        setLocation(location: Point): void;
        search(keywords: string): void;
        getResults(): AutocompleteResult;
        setInputValue(keyword: string): void;
        dispose(): void;

        // Events
        // onconfirm 	{type,target,item}
        // onhighlight 	{type,target,fromitem,toitem}
    }

    export interface AutocompleteOptions {
        location?: string;
        location?: Map;
        location?: Point;
        //location?: string,Map,Point;
        types?: string;
        onSearchComplete?: Function;
        input?: string;
        input?: HTMLElement;
        //input?: string,HTMLElement;
    }

    export interface AutocompleteResultPoi {
        // Property
        province?: string;
        city?: string;
        district?: string;
        street?: string;
        streetNumber?: string;
        business?: string;
    }

    export class AutocompleteResult {
        // Method
        getPoi(i: number): AutocompleteResultPoi;
        getNumPois(): number;

        // Property
        keyword: string;
    }

    export class Boundary {
        // Constructor
        constructor();

        // Method
        get(name:String, callback: Function): void;
    }

    export class Line {
        // Method
        getNumViaStops(): number;
        getGetOnStop(): LocalResultPoi;
        getGetOffStop(): LocalResultPoi;

        // Property
        title: string;
        type: LineType;
    }


    //=====================================================
    //================= Panoramic class ===================
    //=====================================================

    export class Panorama {
        // Constructor
        constructor(container: string, opts?: PanoramaOptions);
        constructor(container: HTMLElement, opts?: PanoramaOptions);

        // Method
        getLinks(): PanoramaLink[];
        getId(): string;
        getPosition(): Point;
        getPov(): PanoramaPov;
        getZoom(): number;
        setId(id: string): void;
        setPosition(position: Point): void;
        setPov(pov: PanoramaPov): void;
        setZoom(zoom: number): void;
        enableScrollWheelZoom(): void;
        disableScrollWheelZoom(): void;
        show(): void;
        hide(): void;

        // Events
        // position_changed 	none
        // links_changed 	none
        // zoom_changed 	none
    }

    export interface PanoramaOptions {
        navigationControl?: boolean;
        linksControl?: boolean;
        indoorSceneSwitchControl?: boolean;
        albumsControl?: boolean;
        albumsControlOptions?: AlbumsControlOptions;
    }

    export class PanoramaLink {
        // Property
        description: string;
        heading: number;
        id: string;
    }

    export class PanoramaPov {
        // Property
        heading: number;
        pitch: number;
    }

    export class PanoramaService {
        // Constructor
        constructor();

        // Method
        getPanoramaById(id: string, callback: (data: PanoramaData) => void): void;
        getPanoramaByLocation(point: Point, callback: (data: PanoramaData) => void): void;
        getPanoramaByLocation(point: Point, radius: number, callback: (data: PanoramaData) => void): void;
    }

    export class PanoramaData {
        // Property
        id: string;
        description: string;
        links: PanoramaLink[];
        position: Point;
        tiles: PanoramaTileData;
    }

    export class PanoramaTileData {
        // Property
        centerHeading: number;
        tileSize: Size;
        worldSize: Size;
    }

    export class PanoramaLabel {
        // Constructor
        constructor(content: string, opts?: PanoramaLabelOptions);

        // Method
        setPosition(position: Point): void;
        getPosition(): Point;
        getPov(): PanoramaPov;
        setContent(content: string);
        getContent(): string;
        show(): void;
        hide(): void;
        setAltitude(altitude: number): void;
        getAltitude(): number;
        addEventListener(): void;
        removeEventListener(): void;

        // Events
        // click 	type,target
        // mouseover 	type,target
        // mouseout 	type,target
        // remove 	type,target
    }

    export interface PanoramaLabelOptions {
        position?: Point;
        altitude?: number;
    }

    export interface AlbumsControlOptions {
        anchor?: ControlAnchor;
        offset?: Size;
        maxWidth?: number;
        maxWidth?: string;
        //maxWidth?: number,string;
        imageHeight?: number;
    }

    export enum PanoramaSceneType {
        BMAP_PANORAMA_INDOOR_SCENE,
        BMAP_PANORAMA_STREET_SCENE
    }

    export enum PanoramaPOIType {
        BMAP_PANORAMA_POI_HOTEL,
        BMAP_PANORAMA_POI_CATERING,
        BMAP_PANORAMA_POI_MOVIE,
        BMAP_PANORAMA_POI_TRANSIT,
        BMAP_PANORAMA_POI_INDOOR_SCENE,
        BMAP_PANORAMA_POI_NONE
    }
}

// Constant
declare var BMAP_API_VERSION: string;

//declare var BMAP_ANCHOR_TOP_LEFT: number;
//declare var BMAP_ANCHOR_TOP_RIGHT: number;
//declare var BMAP_ANCHOR_BOTTOM_LEFT: number;
//declare var BMAP_ANCHOR_BOTTOM_RIGHT: number;
//
//declare var BMAP_UNIT_METRIC: string;
//declare var BMAP_UNIT_IMPERIAL: string;
//
//declare var BMAP_NAVIGATION_CONTROL_LARGE: number;
//declare var BMAP_NAVIGATION_CONTROL_SMALL: number;
//declare var BMAP_NAVIGATION_CONTROL_PAN: number;
//declare var BMAP_NAVIGATION_CONTROL_ZOOM: number;
//
//declare var BMAP_MAPTYPE_CONTROL_HORIZONTAL: number;
//declare var BMAP_MAPTYPE_CONTROL_DROPDOWN: number;
//declare var BMAP_MAPTYPE_CONTROL_MAP: number;
//
//declare var BMAP_STATUS_SUCCESS: number;
//declare var BMAP_STATUS_CITY_LIST: number;
//declare var BMAP_STATUS_UNKNOWN_LOCATION: number;
//declare var BMAP_STATUS_UNKNOWN_ROUTE: number;
//declare var BMAP_STATUS_INVALID_KEY: number;
//declare var BMAP_STATUS_INVALID_REQUEST: number;
//declare var BMAP_STATUS_PERMISSION_DENIED: number;
//declare var BMAP_STATUS_SERVICE_UNAVAILABLE: number;
//declare var BMAP_STATUS_TIMEOUT: number;
//
//declare var BMap_Symbol_SHAPE_CIRCLE: number;
//declare var BMap_Symbol_SHAPE_RECTANGLE: number;
//declare var BMap_Symbol_SHAPE_RHOMBUS: number;
//declare var BMap_Symbol_SHAPE_STAR: number;
//declare var BMap_Symbol_SHAPE_BACKWARD_CLOSED_ARROW: number;
//declare var BMap_Symbol_SHAPE_FORWARD_CLOSED_ARROW: number;
//declare var BMap_Symbol_SHAPE_BACKWARD_OPEN_ARROW: number;
//declare var BMap_Symbol_SHAPE_FORWARD_OPEN_ARROW: number;
//declare var BMap_Symbol_SHAPE_POINT: number;
//declare var BMap_Symbol_SHAPE_PLANE: number;
//declare var BMap_Symbol_SHAPE_CAMERA: number;
//declare var BMap_Symbol_SHAPE_WARNING: number;
//declare var BMap_Symbol_SHAPE_SMILE: number;
//declare var BMap_Symbol_SHAPE_CLOCK: number;
//
//declare var BMAP_ANIMATION_DROP: number;
//declare var BMAP_ANIMATION_BOUNCE: number;
//
//declare var BMAP_POINT_SHAPE_CIRCLE: number;
//declare var BMAP_POINT_SHAPE_STAR: number;
//declare var BMAP_POINT_SHAPE_SQUARE: number;
//declare var BMAP_POINT_SHAPE_RHOMBUS: number;
//declare var BMAP_POINT_SHAPE_WATERDROP: number;
//
//declare var BMAP_POINT_SIZE_TINY: number;
//declare var BMAP_POINT_SIZE_SMALLER: number;
//declare var BMAP_POINT_SIZE_SMALL: number;
//declare var BMAP_POINT_SIZE_NORMAL: number;
//declare var BMAP_POINT_SIZE_BIG: number;
//declare var BMAP_POINT_SIZE_BIGGER: number;
//declare var BMAP_POINT_SIZE_HUGE: number;
//
//declare var BMAP_CONTEXT_MENU_ICON_ZOOMIN: string;
//declare var BMAP_CONTEXT_MENU_ICON_ZOOMOUT: string;
//
//declare var BMAP_NORMAL_MAP: BMap.MapType;
//declare var BMAP_PERSPECTIVE_MAP: BMap.MapType;
//declare var BMAP_SATELLITE_MAP: BMap.MapType;
//declare var BMAP_HYBRID_MAP: BMap.MapType;
//
//declare var BMAP_POINT_DENSITY_HIGH: number;
//declare var BMAP_POINT_DENSITY_MEDIUM: number;
//declare var BMAP_POINT_DENSITY_LOW: number;
//
//declare var BMAP_PANORAMA_INDOOR_SCENE: string;
//declare var BMAP_PANORAMA_STREET_SCENE: string;
//
//declare var BMAP_PANORAMA_POI_HOTEL: string;
//declare var BMAP_PANORAMA_POI_CATERING: string;
//declare var BMAP_PANORAMA_POI_MOVIE: string;
//declare var BMAP_PANORAMA_POI_TRANSIT: string;
//declare var BMAP_PANORAMA_POI_INDOOR_SCENE: string;
//declare var BMAP_PANORAMA_POI_NONE: string;
//
//declare var BMAP_POI_TYPE_NORMAL: number;
//declare var BMAP_POI_TYPE_BUSSTOP: number;
//declare var BMAP_POI_TYPE_SUBSTOP: number;
//
//declare var BMAP_TRANSIT_POLICY_LEAST_TIME: number;
//declare var BMAP_TRANSIT_POLICY_LEAST_TRANSFER: number;
//declare var BMAP_TRANSIT_POLICY_LEAST_WALKING: number;
//declare var BMAP_TRANSIT_POLICY_AVOID_SUBWAYS: number;
//
//declare var BMAP_LINE_TYPE_BUS: number;
//declare var BMAP_LINE_TYPE_SUBWAY: number;
//declare var BMAP_LINE_TYPE_FERRY: number;
//
//declare var BMAP_DRIVING_POLICY_LEAST_TIME: number;
//declare var BMAP_DRIVING_POLICY_LEAST_DISTANCE: number;
//declare var BMAP_DRIVING_POLICY_AVOID_HIGHWAYS: number;
//
//declare var BMAP_ROUTE_TYPE_DRIVING: number;
//declare var BMAP_ROUTE_TYPE_WALKING: number;
//
//declare var BMAP_HIGHLIGHT_STEP: number;
//declare var BMAP_HIGHLIGHT_ROUTE: number;