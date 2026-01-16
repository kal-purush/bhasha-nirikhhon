// Type definitions for SVG
// Project: http://www.w3.org/TR/SVG/
// Definitions by: Daniel Buslowicz <https://github.com/Draccoz>
// Definitions: https://github.com/Draccoz/SVG-Definitions

interface SVGMatrix extends Object {
    a : number;
    b : number;
    c : number;
    d : number;
    e : number;
    f : number;
    flipX : () => SVGMatrix;
    flipY : () => SVGMatrix;
    inverse : () => SVGMatrix;
    multiply : (secondMatrix : SVGMatrix) => SVGMatrix;
    rotate : (angle : number) => SVGMatrix;
    rotateFromVector : (x : number, y : number) => SVGMatrix;
    scale : (scaleFactor : number) => SVGMatrix;
    scaleNonUniform : (scaleFactorX : number, scaleFactorY : number) => SVGMatrix;
    skewX : (angle : number) => SVGMatrix;
    skewY : (angle : number) => SVGMatrix;
    translate : (x : number, y : number) => SVGMatrix;
}

interface SVGTransform extends Object {
    angle : number;
    matrix : SVGMatrix;
    type : number;
    SVG_TRANSFORM_MATRIX : number;
    SVG_TRANSFORM_ROTATE : number;
    SVG_TRANSFORM_SCALE : number;
    SVG_TRANSFORM_SKEWX : number;
    SVG_TRANSFORM_SKEWY : number;
    SVG_TRANSFORM_TRANSLATE : number;
    SVG_TRANSFORM_UNKNOWN : number;
    constructor : () => any;
    setMatrix : (matrix : SVGMatrix) => void;
    setRotate : (angle : number, cx : number, cy : number) => void;
    setScale : (sx : number, sy : number) => void;
    setSkewX : (angle : number) => void;
    setSkewY : (angle : number) => void;
    setTranslate : (tx : number, ty : number) => void;
}

interface SVGTransformList extends Object {
    length : number;
    numberOfItems : number;
    appendItem : (transform : SVGTransform) => SVGTransform;
    clear : () => void;
    consolidate : () => SVGTransform;
    constructor : () => void;
    createSVGTransformFromMatrix : (matrix : SVGMatrix) => SVGTransform;
    getItem : (index : number) => SVGTransform;
    initialize : (newItem : SVGTransform) => SVGTransform;
    insertItemBefore : () => SVGTransform;
    removeItem : () => SVGTransform;
    replaceItem : (transform : SVGTransform, index : number) => SVGTransform;
}

interface SVGAnimatedTransformList extends Object {
    animVal : SVGTransformList;
    baseVal : SVGTransformList;
}

interface SVGGraphicsElement extends SVGElement {
    transform : SVGAnimatedTransformList;
    getBBox : () => any;
    getCTM : () => any;
    getScreenCTM : () => any;
    getTransformToElement : () => any;
    hasExtension : () => any;
}

interface SVGGeometryElement extends SVGGraphicsElement {
    isPointInFill : () => any;
    isPointInStroke : () => any;
}

interface SVGSVGElement extends SVGGraphicsElement, EventTarget {
    suspendRedraw : (maxWaitMilliseconds : number) => number;
    unsuspendRedraw : (suspendHandleID : number) => void;
    unsuspendRedrawAll : () => void;
    forceRedraw : () => void;
    pauseAnimations : () => void;
    unpauseAnimations : () => void;
    animationsPaused : () => boolean;
    getCurrentTime : () => number;
    setCurrentTime : (seconds : number) => void;
    getIntersectionList : (rect : SVGRect, referenceElement : SVGElement) => NodeList;
    getEnclosureList : (rect : SVGRect, referenceElement : SVGElement) => NodeList;
    checkIntersection : (element : SVGElement, rect : SVGRect) => boolean;
    checkEnclosure : (element : SVGElement, rect : SVGRect) => boolean;
    deselectAll : () => void;
    createSVGNumber : () => SVGNumber;
    createSVGLength : () => SVGLength;
    createSVGAngle : () => SVGAngle;
    createSVGPoint : () => SVGPoint;
    createSVGMatrix : () => SVGMatrix;
    createSVGRect : () => SVGRect;
    createSVGTransform : () => SVGTransform;
    createSVGTransformFromMatrix : (matrix : SVGMatrix) => SVGTransform;
    getElementById : (elementId : String) => Element;

    x : SVGAnimatedLength
    y : SVGAnimatedLength
    width : SVGAnimatedLength
    height : SVGAnimatedLength
    contentScriptType : String
    contentStyleType : String
    viewport : SVGRect
    pixelUnitToMillimeterX : number
    pixelUnitToMillimeterY : number
    screenPixelToMillimeterX : number
    screenPixelToMillimeterY : number
    useCurrentView : boolean
    currentView : SVGViewSpec
    currentTranslate : SVGPoint
    currentScale : number
}

interface SVGRectElement extends SVGGraphicsElement {
    x : SVGAnimatedLength;
    y : SVGAnimatedLength;
    width : SVGAnimatedLength;
    height : SVGAnimatedLength;
    rx : SVGAnimatedLength;
    ry : SVGAnimatedLength;
}

interface SVGGElement extends SVGGraphicsElement, Node {

}

interface SVGElement extends Element {
    id : string;
    xmlbase : string;
    ownerSVGElement : SVGSVGElement;
    viewportElement : SVGElement;
}

interface SVGViewSpec extends Object {
    preserveAspectRatio : SVGAnimatedPreserveAspectRatio;
    preserveAspectRatioString : string;
    transform : SVGTransformList;
    transformString : string;
    viewBox : SVGAnimatedRect;
    viewBoxString : string;
    viewTarget : void;
    viewTargetString : string;
    zoomAndPan : number;
}