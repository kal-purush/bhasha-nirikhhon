
module Svg {

    enum ShapeType {
        Circle,
        Rect,
        Line,
        Poly
    }

    export interface IStyle {
        fill: string;
        stroke: string;
        "stroke-width": string;
    }

    export interface IPoint {
        x: number;
        y: number;
    }

    export abstract class Shape{
        protected svgElement: Element;
        protected svgns = "http://www.w3.org/2000/svg";
        protected parameters: string;

        constructor(protected x: number, protected y: number, protected style?: string) { }

        abstract setSvgElement();

        setStyle(s: IStyle) {
            var keys = Object.keys(s);
            keys.forEach((v, i, a) => this.svgElement.setAttribute(v, s[v]));
        }

        getSvg(): Element {
            return this.svgElement;
        }
    }

    export class Rectangle extends Shape{
        public width: number;
        public height: number;

        constructor(x: number, y: number, width: number, height: number) {
            super(x, y, null);
            this.width = width;
            this.height = height;
            this.svgElement = document.createElementNS(this.svgns, "rect");
            this.setSvgElement();
        }

        setSvgElement() {
            this.svgElement.setAttribute("x", <any>this.x);
            this.svgElement.setAttribute("y", <any>this.y);
            this.svgElement.setAttribute("width", <any>this.width);
            this.svgElement.setAttribute("height", <any>this.height);
        }

        update(x?: number, y?: number, width?: number, height?: number) {
            this.x = x === undefined? this.x: x;
            this.y = y === undefined ? this.y : y;
            this.width = width === undefined ? this.width : width;
            this.height = height === undefined? this.height:height;
            this.setSvgElement();
        }
    }

    export class Circle extends Shape {
        public r: number;

        constructor(x: number, y: number, r: number) {
            super(x, y, null);
            this.r = r;
            this.svgElement = document.createElementNS(this.svgns, "circle");
            this.setSvgElement();
        }

        setSvgElement() {
            this.svgElement.setAttribute("x", <any>this.x);
            this.svgElement.setAttribute("t", <any>this.y);
            this.svgElement.setAttribute("r", <any>this.r);
        }

        update(x?: number, y?: number, r?: number) {
            this.x = x === undefined ? this.x : x;
            this.y = y === undefined ? this.y : y;
            this.r = r === undefined ? this.r : r;
            this.setSvgElement();
        }
    }

    export class Line extends Shape{
        public xF: number;
        public yF: number;

        constructor(x: number, y: number, xF: number, yF: number) {
            super(x, y, null);
            this.xF = xF;
            this.yF = yF;
            this.svgElement = document.createElementNS(this.svgns, "line");
            this.setSvgElement();
        }

         setSvgElement() {
            this.svgElement.setAttribute("x1", <any>this.x);
            this.svgElement.setAttribute("y2", <any>this.y);
            this.svgElement.setAttribute("x2", <any>this.xF);
            this.svgElement.setAttribute("y2", <any>this.yF);
        }
    }

    export class PolyLine extends Shape {
        public points: Array<IPoint>;

        constructor(x: number, y: number, points: Array<IPoint>) {
            super(x, y);
            this.points = new Array<IPoint>();
            this.points = points;
        }

        setSvgElement() {
            this.svgElement.setAttribute("x", <any> this.x);
            this.svgElement.setAttribute("y", <any> this.y);
            this.svgElement.setAttribute("points", <any> this.points);
        }

        update(x: number, y: number, points: Array<IPoint>) {
            this.x = this.x === undefined ? this.x : x;
            this.y = this.y === undefined ? this.y : y;
            this.points = this.points === undefined ? this.points : points;
        }

    }
}

module GraphElement {
    export class Axis {
        public origin: any;
        public xMax: number;
        public yMax: number;
        public xAxis: Svg.Line;
        public yAxis: Svg.Line;

        constructor(origin: any, xMax: number, yMax: number) {
            this.origin = origin;
            this.xMax = xMax;
            this.yMax = yMax;
            this.xAxis = new Svg.Line(origin.x, origin.y, xMax, origin.y);
            this.yAxis = new Svg.Line(origin.x, origin.y, origin.x, origin.y);
            var style = ({
                fill: "black",
                stroke: "black",
                "stroke-width": "3"
            });
            this.xAxis.setStyle(style);
            this.yAxis.setStyle(style);
        }

        getElement(): any {
            return {
                x: this.xAxis.getSvg(),
                y: this.yAxis.getSvg()
            };
        }
    }

    export abstract class ElementBase<T> {
        constructor(public element: T, public data: number) { }

        abstract getElement(): Element;
    }

    export class Bar extends ElementBase<Svg.Rectangle> {

        constructor(rectangle: Svg.Rectangle, data: number) {
            super(rectangle, data);
        }

        getElement(): Element {
            return this.element.getSvg();
        }
    }
}

module HelloGraph{

    interface IGraphData{
    }

    function scale(list: Array<any>, o) {
        var maxValue = Math.max.apply(null, list);
        var minValue = Math.min.apply(null, list);
        var maxScaled = o.origin;
        var minScaled = 0;
        var difference = maxValue - minValue;
        var differenceScaled = maxScaled - minScaled;
        var listScaled = list.map(n => ((n - minValue) * differenceScaled) / (difference + minScaled));
        return listScaled;
    }

    export class GraphBase {
        public scaledList: Array<number>;
        public width: number;
        public height: number;
        public origin: number;
        public viewBox: string;

        constructor(public list: Array<any>, public svg: any) {
            this.width = svg.parentElement.clientWidth;
            this.height = svg.parentElement.clientHeight;
            this.origin = this.height / 2;
            this.scaledList = scale(this.list, this);
        }
    }

    export class Bars extends GraphBase{
        public bars = new Array<GraphElement.Bar>();
        public axis: GraphElement.Axis;
        public spacing: number;
        public barWidth: number;

        constructor(list: Array<number>, svg: Element) {
            super(list, svg);
            this.barWidth = 30;
            this.spacing = (550 / list.length);
            this.svg.setAttribute("viewBox", 0 + " " + 0 + " " + this.width + " " + this.height);
            this.setGraph();
        }

        setGraph() {
            let x = 250;

            for (let i = 0; i < this.scaledList.length; i++) {
                let rectangle: Svg.Rectangle;

                if (this.list[i] > 0) {
                    rectangle = new Svg.Rectangle(x, this.origin - this.scaledList[i], this.barWidth, this.scaledList[i]) ;
                } else {
                    rectangle = new Svg.Rectangle(x, this.origin, this.barWidth, this.scaledList[i]);
                    console.log(this.height - this.scaledList[i]);
                }

                this.bars.push(new GraphElement.Bar(rectangle, this.list[i]));
                x += this.spacing;
            }
        }

        draw() {
            for (let i = 0; i < this.scaledList.length; i++) {
                this.svg.appendChild(this.bars[i].getElement());
            }
        }
    }

}