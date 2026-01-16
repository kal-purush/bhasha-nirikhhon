import {Tool} from './Tool';
import {PencilOptions} from "./PencilOptions"

export class Pencil implements Tool {
	protected state: string = 'up';
	private color: String = '#00000000';
	public width: number = 10;
	public linecap: string = 'round';
	
	start(ctx: CanvasRenderingContext2D, x: number, y: number) {
		if (!ctx) return;
		this.state = 'down',
		ctx.lineWidth = this.width;
		ctx.lineCap = this.linecap;
		ctx.strokeStyle = this.color;
		ctx.globalCompositeOperation = 'source-over';
		ctx.beginPath();
		ctx.moveTo(x, y);
	}
	
	update(ctx: CanvasRenderingContext2D, x: number, y: number) {
		if (!ctx) return;
		if (this.state == 'down') {
			ctx.lineTo(x, y);
			ctx.stroke();
		}
	}
	
	stop(ctx: CanvasRenderingContext2D, x: number, y: number) {
		if (!ctx) return;
		this.state = 'up',
		ctx.lineTo(x, y);
		ctx.stroke();
		ctx.closePath();
	}
	
	getName() {
		return 'Brush';
	}
	
	getIcon() {
		return 'paint-brush';
	}
	
	getOptionsComponent() {
		return PencilOptions
	}
}