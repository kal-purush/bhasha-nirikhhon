import BladeMaterial from "BladeMaterial";

export default class BaldeWoodMaterial extends BladeMaterial {
	constructor(
		public name: string,
		public janka: number | [number, number],
		public specificGravity: number | [number, number],
		public poundPerCubic: number| [number, number]
		)
	{
		super(name);
	}

	/**
	 * Create a specific instane of this type of material. 
	 */
	createInstance() {
		var result = new BaldeWoodMaterial(
			this.name,
			typeof this.janka !== "number"? getRandom(<[number, number]>this.janka): this.janka,
			typeof this.specificGravity !== "number"? getRandom(<[number, number]>this.specificGravity): this.specificGravity,
			typeof this.poundPerCubic !== "number"? getRandom(<[number, number]>this.poundPerCubic): this.poundPerCubic
		);
		
		result.hardness = <number>result.janka / 10;
		result.stiffness = <number>result.specificGravity * 100;
		
		return result;
		
		function getRandom(tuple: [number, number]) {
			return tuple[0] + Math.random()*(tuple[1] - tuple[0]);
		}
	}	
}