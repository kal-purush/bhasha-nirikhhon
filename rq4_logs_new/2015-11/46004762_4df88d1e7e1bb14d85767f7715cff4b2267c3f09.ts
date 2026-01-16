/// <reference path="modulove.d.ts"/>
/// <reference path="grids/class.flexibleGrid.ts"/>
/// <reference path="location/class.location.simple.ts"/>
/// <reference path="grids/class.appender.ts"/>
/// <reference path="mapper/class.mapper.width.ts"/>
/// <reference path="mapper/class.mapper.height.ts"/>

module modulove {

	import IGrid = grids.IGrid;
	import IConvert = grids.IConvert;
	import GridRenderer = grids.Appender;

	export class Modulove {
		renderer:GridRenderer;
		locations:Array<grids.ILocation>;

		constructor(containerSettings:grids.IContainerSettings, data:Array<grids.ILocation>) {
			var mapper,
				convert = new modulove.Convert(containerSettings.step);

			if (containerSettings.width) {
				mapper = new grids.Width(convert.toPoint(containerSettings.width), convert.toPoint(containerSettings.height));
			} else {
				mapper = new grids.Height(convert.toPoint(containerSettings.height));
			}

			var gridSystem = new grids.FlexibleGrid(mapper);

			this.renderer = new grids.Appender(gridSystem, convert, data);
			this.locations = this.renderer.locations;
		}

		getLocations() {
			return this.locations;
		}
	}

	export class Convert implements IConvert {
		step:number;

		constructor(step:number) {
			this.step = step;
		}

		toPx(points:number):number {
			return points * this.step;
		}

		toPoint(px:number):number {
			return Math.floor(px / this.step);
		}

	}
}