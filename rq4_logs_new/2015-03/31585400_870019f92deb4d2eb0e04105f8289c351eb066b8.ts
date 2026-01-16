/// <reference path='../../lib/jQuery.d.ts'/>

/// <reference path='../../lib/illa/_module.ts'/>
/// <reference path='../../lib/illa/Arrkup.ts'/>
/// <reference path='../../lib/illa/Axis2D.ts'/>
/// <reference path='../../lib/illa/End.ts'/>
/// <reference path='../../lib/illa/Log.ts'/>

/// <reference path='../../lib/berek/Widget.ts'/>

/// <reference path='canvas/Canvas.ts'/>
/// <reference path='path/BezierPath.ts'/>
/// <reference path='path/Path.ts'/>

module ecset {
	export class Main {
		
		private static instance = new Main();
		
		private canvas: canvas.Canvas;
		
		constructor() {
			jQuery(illa.bind(this.onDomLoaded, this));
		}
		
		onDomLoaded(): void {
			illa.Log.info('DOM loaded.');
			
//			var p = new path.Path([new path.Point(255, 255), new path.Point(320, 500), new path.Point(630, 320), new path.Point(1024-255, 768-255)]);
			var p = new path.BezierPath([
				new path.BezierPoint(255, 255, 255, 255, 255, 768),
				new path.BezierPoint(1024-255, 768-255, 1024-255, 0, 1024-255, 1024-255)
			]);
			
			this.canvas = new canvas.Canvas(jQuery('body'), 1024, 768);
			this.canvas.drawPath(p.linearize(10));
		}
	}
}