import Armor from 'src/models/armor';

const armorCatalogue: Armor[] = [
	{
		name: 'Heavy leather helmet',
		isHard: false,
		stoppingPower: 4,
		encumberanceValue: 0,
		cost: 50,
		head: true
	},
	{
		name: 'Heavy leather vest',
		isHard: false,
		stoppingPower: 4,
		encumberanceValue: 0,
		cost: 50,
		torso: true
	},
	{
		name: 'Heavy leather sleeves',
		isHard: false,
		stoppingPower: 4,
		encumberanceValue: 0,
		cost: 50,
		arms: true
	},
	{
		name: 'Heavy leather pants',
		isHard: false,
		stoppingPower: 4,
		encumberanceValue: 0,
		cost: 50,
		legs: true
	},
	{
		name: 'Kevlar T-shirt, Vest',
		isHard: false,
		stoppingPower: 10,
		encumberanceValue: 0,
		cost: 90,
		torso: true
	},
	{
		name: 'Steel helmet',
		isHard: true,
		stoppingPower: 14,
		encumberanceValue: 0,
		cost: 20,
		head: true
	},
	{
		name: 'Light Armor jacket',
		isHard: false,
		stoppingPower: 14,
		encumberanceValue: 0,
		cost: 150,
		torso: true,
		arms: true
	},
	{
		name: 'Med Armor jacket',
		isHard: false,
		stoppingPower: 18,
		encumberanceValue: 1,
		cost: 200,
		torso: true,
		arms: true
	},
	{
		name: 'Flack vest',
		isHard: true,
		stoppingPower: 20,
		encumberanceValue: 1,
		cost: 200,
		torso: true
	},
	{
		name: 'Flack pants',
		isHard: true,
		stoppingPower: 20,
		encumberanceValue: 1,
		cost: 200,
		legs: true
	},
	{
		name: 'Nylon Helmet',
		isHard: true,
		stoppingPower: 20,
		encumberanceValue: 0,
		cost: 100,
		head: true
	},
	{
		name: 'Heavy Armor jacket',
		isHard: true,
		stoppingPower: 20,
		encumberanceValue: 2,
		cost: 250,
		torso: true,
		arms: true
	},
	{
		name: "Door Gunner's vest",
		isHard: true,
		stoppingPower: 25,
		encumberanceValue: 3,
		cost: 250,
		torso: true
	},
	{
		name: 'MetalGear™',
		isHard: true,
		stoppingPower: 25,
		encumberanceValue: 2,
		cost: 600,
		head: true,
		torso: true,
		arms: true,
		legs: true
	},
	{
		name: 'Subdermal Armor',
		isHard: false,
		stoppingPower: 18,
		encumberanceValue: 0,
		cost: 1200,
		torso: true
	},
	{
		name: 'Skin Weave',
		isHard: false,
		stoppingPower: 12,
		encumberanceValue: 0,
		cost: 2000,
		head: true,
		torso: true,
		arms: true,
		legs: true
	},
	{
		name: 'Cyberware Cowl',
		isHard: false,
		stoppingPower: 25,
		encumberanceValue: 0,
		cost: 200,
		head: true
	},
	{
		name: 'Cyberware Faceplate',
		isHard: false,
		stoppingPower: 25,
		encumberanceValue: 0,
		cost: 400,
		head: true
	},
	{
		name: 'Cyberware Torsoplate',
		isHard: false,
		stoppingPower: 25,
		encumberanceValue: 0,
		cost: 2000,
		torso: true
	}
];

export default armorCatalogue;