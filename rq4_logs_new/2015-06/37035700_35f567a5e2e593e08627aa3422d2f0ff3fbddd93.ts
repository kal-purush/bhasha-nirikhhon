interface PlaneTemplateMap{
	[index: string]: Part[];
}

interface IPlane{
	_id:string;
	type:string;
	parts:Part[];
	printState:number;
	lastModified:Date;
	name:string;
	disabled:boolean;
    info:{
      email:string,
      pcode:string,
      newsletter:boolean,
      emailSent:Date
    }
}

interface Part{
	name: string;
	path: string;
	width: number;
	height: number;
	position2D?: {
	    x: number,
	    y: number
	};
	position3D?: {
	    x: number,
	    y: number,
	    z: number
	};
	rotation3D?: {
	    x: number,
	    y: number,
	    z: number
	};
	textureBitmap?:string
	textureTop?:boolean;
	textureBottom?:boolean;
	textureFlipY?:boolean;
	decals?:Decal[];
	drawTexture?:boolean;
}
interface Decal{
	x:number;
	y:number;
	angle:number;
	text?:string;
	size?:number;
	path?:string;
	locked?:string;
}