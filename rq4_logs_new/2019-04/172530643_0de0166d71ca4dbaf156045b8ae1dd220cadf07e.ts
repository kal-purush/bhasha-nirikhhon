import { IconDefinition, IconPrefix, IconName } from '@fortawesome/fontawesome-common-types';

export const prefix: IconPrefix = 'fab';
export const iconName: IconName = 'nicoru';
export const width: number = 64;
export const height: number = 64;
export const ligatures: string[] = [];
export const unicode: string = 'f334';
export const svgPathData: string = 'M32,64C14.35,64,0,49.64,0,32C0,14.35,14.35,0,32,0c17.64,0,32,14.35,32,32C64,49.64,49.64,64,32,64z M32,4C16.56,4,4,16.56,4,32s12.56,28,28,28s28-12.56,28-28S47.44,4,32,4z M20,14c0-2.21,1.79-4,4-4s4,1.79,4,4v12c0,2.21-1.79,4-4,4s-4-1.79-4-4V14z M41,12L41,12c2.21,0,4,1.79,4,4v8c0,2.21-1.79,4-4,4l0,0c-2.21,0-4-1.79-4-4v-8C37,13.79,38.79,12,41,12z M20,38c0-2.21,1.79-4,4-4s4,1.79,4,4v6c0,2.21,1.79,4,4,4h1c2.21,0,4-1.79,4-4v-6c0-2.21,1.79-4,4-4s4,1.79,4,4v6c0,5.52-4.48,10-10,10h-5c-5.52,0-10-4.48-10-10V38z';

export const faNicoru: IconDefinition = {
	prefix,
	iconName,
	icon: [
		width,
		height,
		ligatures,
		unicode,
		svgPathData
	]
};