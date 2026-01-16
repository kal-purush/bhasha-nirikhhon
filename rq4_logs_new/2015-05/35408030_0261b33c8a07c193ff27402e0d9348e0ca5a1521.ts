var svgns = "http://www.w3.org/2000/svg";

interface Drawable {
    draw(svg: SVGSVGElement, depth: number);
}

class Triangle implements Drawable {
    constructor(private x: number, private y: number, private side: number, private rot: number) {
    }

    draw(svg: SVGSVGElement, depth: number) {
        if (!depth)
            return;

        var half = this.side / 2;

        new Square(this.x, this.y, half, this.rot).draw(svg, depth - 1);
        new Triangle(this.x - Math.sin(this.rot) * half, this.y + Math.cos(this.rot) * half, half, this.rot).draw(svg, depth - 1);
        new Triangle(this.x + Math.cos(this.rot) * half, this.y + Math.sin(this.rot) * half, half, this.rot).draw(svg, depth - 1);

        var tri: SVGPolygonElement = <SVGPolygonElement>document.createElementNS(svgns, 'polygon');

        var p = svg.createSVGPoint();
        p.x = this.x;
        p.y = this.y;
        tri.points.appendItem(p);

        p = svg.createSVGPoint();
        p.x = this.x + this.side;
        p.y = this.y;
        tri.points.appendItem(p);

        p = svg.createSVGPoint();
        p.x = this.x;
        p.y = this.y + this.side;
        tri.points.appendItem(p);

        var transform = svg.createSVGTransform();
        transform.setRotate(this.rot * 180 / Math.PI, this.x, this.y);
        tri.transform.baseVal.appendItem(transform);

        tri.style.fillOpacity = "0";
        tri.style.stroke = "black";

        svg.appendChild(tri);
    }
}

class Square implements Drawable {
    constructor(private x: number, private y: number, private side: number, private rot: number) {
    }

    draw(svg: SVGSVGElement, depth: number) {
        if (!depth)
            return;

        new Square(
            this.x + Math.cos(this.rot) * this.side / 2,
            this.y + Math.sin(this.rot) * this.side / 2,
            this.side / Math.SQRT2,
            this.rot + Math.PI / 4).draw(svg, depth - 1);

        new Triangle(
            this.x,
            this.y,
            this.side / 2,
            this.rot).draw(svg, depth - 1);

        new Triangle(
            this.x + Math.cos(this.rot) * this.side,
            this.y + Math.sin(this.rot) * this.side,
            this.side / 2,
            this.rot + Math.PI / 2).draw(svg, depth - 1);

        new Triangle(
            this.x + Math.cos(this.rot) * this.side - Math.sin(this.rot) * this.side,
            this.y + Math.sin(this.rot) * this.side + Math.cos(this.rot) * this.side,
            this.side / 2,
            this.rot + 2 * Math.PI / 2).draw(svg, depth - 1);

        new Triangle(
            this.x - Math.sin(this.rot) * this.side,
            this.y + Math.cos(this.rot) * this.side,
            this.side / 2,
            this.rot + 3 * Math.PI / 2).draw(svg, depth - 1);

        var rect: SVGRectElement = <SVGRectElement>document.createElementNS(svgns, 'rect');

        rect.x.baseVal.value = this.x;
        rect.y.baseVal.value = this.y;
        rect.height.baseVal.value = this.side;
        rect.width.baseVal.value = this.side;

        var transform = svg.createSVGTransform();
        transform.setRotate(this.rot * 180 / Math.PI, this.x, this.y);
        rect.transform.baseVal.appendItem(transform);

        rect.style.fillOpacity = "0";
        rect.style.stroke = "black";

        svg.appendChild(rect);
    }
}

window.onload = () => {
    var query = window.location.search;
    var depth = +query.split("depth=")[1];
    if (depth) {
        var input = <HTMLInputElement>document.getElementById("depth");
        input.value = "" + depth;
    } else {
        depth = 3;
    }

    var svg: SVGSVGElement = <SVGSVGElement><any>document.getElementById('svg');

    new Square(0, 0, 800, 0).draw(svg, depth);
};