import d3 from 'd3';

export type Scale = d3.scale.Ordinal<string, number> | d3.scale.Linear<number, number>;
export interface Drawer {
    draw(selection: d3.Selection<any>, x: Scale, y: Scale, width: number, height: number): void;
}

enum axisMode {
    range,
    domain,
}


export class AxisBuilder {
    private xHasTicks = false;
    private yHasTicks = false;
    private xTicks = 0;
    private yTicks = 0;

    private xMode = axisMode.range;
    private xRange = [0, 0];
    private xLabels: string[] = [];

    private yMode = axisMode.range;
    private yRange = [0, 0];
    private yLabels: string[] = [];
    private hidden = false;

    public withXLabels(labels: string[]) {
        this.xMode = axisMode.domain;
        this.xLabels = labels;
        return this;
    }

    public withYRange(start: number, end: number) {
        this.yMode = axisMode.range;
        this.yRange = [start, end];
        return this;
    }

    public withXTicks(howMany = 0) {
        this.xHasTicks = true;
        this.xTicks = howMany;
        return this;
    }

    public withYTicks(howMany = 0) {
        this.yHasTicks = true;
        this.yTicks = howMany;
        return this;
    }

    public hide(hide = true) {
        this.hidden = hide;
        return this;
    }

    public getXYScales(width: number, height: number) {
        let x: Scale = null;
        switch (this.xMode) {
            case axisMode.domain:
                x = d3.scale.ordinal().domain(this.xLabels).rangePoints([0, width]);

            case axisMode.range:
                x = d3.scale.linear().range([width, 0]).domain(this.xRange);
        }

        let y: Scale = null;
        switch (this.yMode) {
            case axisMode.domain:
                y = d3.scale.ordinal().domain(this.yLabels).rangePoints([0, height]);

            case axisMode.range:
                y = d3.scale.linear().range([height, 0]).domain(this.yRange);
        }

        return { x: x, y: y };
    }

    public draw(svg: d3.Selection<any>, x: Scale, y: Scale, width: number, height: number) {
        if (this.hide) return;

        let xAxis = d3.svg.axis().scale(x).orient("bottom");

        if (this.xHasTicks)
            xAxis.innerTickSize(-height).outerTickSize(0).tickPadding(10)

        if (this.xTicks > 0)
            xAxis.ticks(this.xTicks);

        let yAxis = d3.svg.axis().scale(y).orient("left");

        if (this.yHasTicks)
            yAxis.innerTickSize(-width).outerTickSize(0).tickPadding(10);

        if (this.yTicks > 0)
            yAxis.ticks(this.yTicks);

        svg.append("g").attr("class", "x axis").attr("transform", "translate(0," + height + ")").call(xAxis);
        svg.append("g").attr("class", "y axis").call(yAxis);
    }
}