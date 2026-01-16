import * as Materials from 'BladeMaterials';

export enum Type {
	Shakehand,
	Penhold,
	Tenaly
}

export enum Handle {
	Penhold,
	Straight,
	Flare,
	Anatomic
}

export enum Plys {
	Five = 5,
	Seven = 7
}

export default class Blade {
	thickness: number
	weight: number
	width: number
	tall: number
	stiffness: number
	hardness: number
	piles: Materials.BladeMaterial[]
	constructor(public type: Type | string, public handle: Handle | string) {
		
	}
	
	get control(): number {
		// TODO: calculate the control of the blade
		return 0;
	}
	
	get speed(): number {
		return 0;
	}
}