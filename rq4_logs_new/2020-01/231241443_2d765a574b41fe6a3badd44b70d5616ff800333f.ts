import { Renderer } from "./renderer";

export class CanvasRenderer extends Renderer {
    
    private _canvas: HTMLCanvasElement;
    private _ctx: any;
    private _cachedBackground: ImageData;
    
    /**
     * Renderer to render the chart on a canvas object
     * @param el DOM element to host the canvas (root of the plugin)
     * @param options options object of the plugin
     */
    constructor(el: HTMLElement, options: object) {
        super(options);
        this._canvas = document.createElement('canvas');
        this._ctx = this._canvas.getContext('2d');
        el.appendChild(this._canvas);
        this._canvas.setAttribute('style', 'position: absolute;top: 0;left: 0;')
        // canvas on retina devices
        if (window.devicePixelRatio > 1) {
            this._canvas.style.width = this._canvas.style.height = [this.options.size, 'px'].join('');
            this._canvas.width = this._canvas.height = this.options.size * this.scaleBy;
            this._ctx.scale(this.scaleBy, this.scaleBy);
        }
        // move 0,0 coordinates to the center
        this._ctx.translate(this.options.size / 2, this.options.size / 2);
        // rotate canvas -90deg
        this._ctx.rotate((-1 / 2 + this.options.rotate / 180) * Math.PI);
        this._cachedBackground = this._ctx.getImageData(0, 0, this.options.size * this.scaleBy, this.options.size * this.scaleBy);
    }

    /**
     * Draw a circle around the center of the canvas
     * @param color Valid CSS Color
     * @param lineWidth Width of the line in px
     * @param percent Percentage to draw (float between -1 and 1)
     */
    drawCircle(color: string, lineWidth: number, percent: number) {
        percent = Math.min(Math.max(-1, percent || 0), 1);
		const isNegative = percent <= 0 ? true : false;

		this._ctx.beginPath();
		this._ctx.arc(0, 0, this.radius, 0, Math.PI * 2 * percent, isNegative);

		this._ctx.strokeStyle = color;
		this._ctx.lineWidth = lineWidth;

		this._ctx.stroke();
    }

    /**
	 * Draw the scale of the chart
	 */
    protected drawScale() {
        let offset;
		let length;
        let width = 1;
		this._ctx.lineWidth = 1;
		this._ctx.fillStyle = this.options.scaleColor;

		this._ctx.save();
		for (let i = 24; i > 0; --i) {
			if (i % 6 === 0) {
				length = this.options.scaleLength * 1.2;
                offset = 0;
                width = 3;
			} else {
				length = this.options.scaleLength * 0.6;
                offset = this.options.scaleLength - length;
                width = 1;
			}
			this._ctx.fillRect(-this.options.size/2 + offset, 0, length, width);
			this._ctx.rotate(Math.PI / 12);
		}
		this._ctx.restore();
    }

    /**
	 * Clear the complete canvas
	 */
    clear() {
        this._ctx.clearRect(this.options.size / -2, this.options.size / -2, this.options.size, this.options.size);
    }

    /**
	 * Draw the complete chart
	 * @param {number} percent Percent shown by the chart between -100 and 100
	 */
    draw(percent: number) {
        this._ctx.lineCap = this.options.lineCap;

		// if barcolor is a function execute it and pass the percent as a value
		let color;
		if (typeof(this.options.barColor) === 'function') {
			color = this.options.barColor(percent);
		} else {
			color = this.options.barColor;
		}

        // draw bar
        this.drawScale();
		this.drawCircle(color, this.options.lineWidth, percent / 100);
    }

    /** Canvas Accessor Getter and Setter */
    public get canvas(): HTMLCanvasElement {
        return this._canvas;
    }
    public set canvas(value: HTMLCanvasElement) {
        this._canvas = value;
    }

    public get ctx(): CanvasRenderingContext2D {
        return this._ctx;
    }
    public set ctx(value: CanvasRenderingContext2D) {
        this._ctx = value;
    }

    public get cachedBackground(): ImageData {
        return this._cachedBackground;
    }
    public set cachedBackground(value: ImageData) {
        this._cachedBackground = value;
    }
}