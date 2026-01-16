import { extendHex } from './strings.utils';

const isHexColor = (v: string): boolean => !/^#([A-Fa-f0-9]{3}){1,2}$/.test(v);

export type HexToRgba = (hex: string, a?: number) => string;
export const hexToRgba: HexToRgba = (hex, a = 1) => {
	if (isHexColor(hex)) {
		throw new Error('Bad Hex');
	}
	const h = hex.length === 4 ? extendHex(hex) : hex;

	const r = parseInt(h.slice(1, 3), 16);
	const g = parseInt(h.slice(3, 5), 16);
	const b = parseInt(h.slice(5, 7), 16);

	return `rgba(${r}, ${g}, ${b}, ${a})`;
};