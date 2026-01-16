interface IWindRoseData {
    colors: string[];
    series?: number[][];
}

interface IWindRoseOptions {
    id: string;
    data: IWindRoseData;
    padding?: number;
}

class WindRose {
    constructor(options: IWindRoseOptions) {
        this.element = document.getElementById(options.id);
        if (!this.element)
            throw "Нет такого элемента";

        //window.onresize = function (e) {
        this.element.onresize = function (e) {
            debugger;
            this.resize();
        }

        if (options.data)
            this.data = options.data;

        this.padding = options.padding || 10;

        this.canvas = document.createElement("canvas");
        this.element.appendChild(this.canvas);

        this.canvas.width = this.element.clientWidth;
        this.canvas.height = this.element.clientHeight;
        
        this.context = this.canvas.getContext("2d");
    }

    draw() {
        var i, j, x, y;

        var center = {
            x: this.canvas.width / 2,
            y: this.canvas.height / 2
        }

        this.clear();

        //  получение максимального количества осей
        var axesCount = 0;
        for (i = 0; i < this.data.series.length; i++)
            axesCount = Math.max(axesCount, this.data.series[i].length);

        console.assert(axesCount !== 0, "осей не может быть 0");

        //  угол между осями
        var dAngle = 2 * Math.PI / axesCount;

        //  максимальное значение серии
        var maxSeriesItem = 0;
        for (i = 0; i < this.data.series.length; i++)
            for (j = 0; j < this.data.series[i].length; j++)
                maxSeriesItem = Math.max(maxSeriesItem, this.data.series[i][j]);

        //  радиус
        var radius = Math.min(this.canvas.height, this.canvas.width) / 2 - this.padding;

        //  коэффициент масштабирования
        var scale = radius / maxSeriesItem * 0.9;

        //  угол начинается сверху
        var angle = -Math.PI / 2;

        //this.context.beginPath();
        //  рисование шкалы
        for (i = 0; i < axesCount; i++) {
            x = radius * Math.cos(angle) + center.x;
            y = radius * Math.sin(angle) + center.y;

            this.context.beginPath();
            this.context.moveTo(center.x, center.y);
            this.context.lineTo(x, y);
            this.context.stroke();

            angle += dAngle;
        }
        
        this.context.stroke();

        //  рисование лучей
        for (i = 0; i < this.data.series.length; i++) {
            //  первый угол -п/2 и он startX, startY
            angle = -Math.PI / 2 + dAngle;

            //  начальная точка
            var startX = this.data.series[i][0] * scale * Math.cos(-Math.PI / 2) + center.x;
            var startY = this.data.series[i][0] * scale * Math.sin(-Math.PI / 2) + center.y;

            this.context.beginPath();
            this.context.save();
            this.context.strokeStyle = this.data.colors[i];
            this.context.lineWidth = 5;
            this.context.moveTo(startX, startY);
            
            for (j = 1; j < this.data.series[i].length; j++) {
                x = this.data.series[i][j] * scale * Math.cos(angle) + center.x;
                y = this.data.series[i][j] * scale * Math.sin(angle) + center.y;

                this.context.lineTo(x, y);

                angle += dAngle;
            }

            this.context.lineTo(startX, startY);
            this.context.stroke();
            this.context.restore();
        }
        

        return this;
    }

    resize() {
        this.canvas.width = this.element.clientWidth;
        this.canvas.height = this.element.clientHeight;
        this.draw();
        return this;
    }

    clear() {
        WindRose.log("clear");
        this.context.clearRect(0, 0, this.canvas.width, this.canvas.height);
        return this;
    }

    static log(msg: string) {
        var log = document.getElementById("log");
        var span = document.createElement("span");
        span.innerHTML = msg;
        log.appendChild(span).appendChild(document.createElement("br"));

    }

    private element: HTMLElement;
    private data: IWindRoseData;
    private canvas: HTMLCanvasElement;
    private context: CanvasRenderingContext2D;
    padding: number;
}