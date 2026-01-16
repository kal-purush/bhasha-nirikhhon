import {EraserOptions} from "./EraserOptions";
import {Pencil} from './Pencil';

export class Eraser extends Pencil {

	start(ctx: CanvasRenderingContext2D, x: number, y: number) {
		if (!ctx) return;
		this.state = 'down',
		ctx.lineWidth = this.width;
		ctx.globalCompositeOperation = 'destination-out';
		ctx.beginPath();
		ctx.moveTo(x, y);
	}
	
	getName() {
		return 'Eraser';
	}
	
	getIcon() {
		return 'eraser';
	}

	getOptions() {
		return {
			width: this.width
		}
	}

	getOptionsComponent() {
		return EraserOptions
	}

}