import { isObject } from "../utils";
import { Parser, Coords } from "../types";

/**
 * Parses a coordinates object.
 */
const parseCoordsObject: Parser = coords => {
	if (!isObject<Coords>(coords)) {
		return null;
	}

	if ([ "x", "y", "z" ].some(key => key in coords)) {
		const { x = 0, y = 0, z = 0 } = coords;
		return [ x, y, z ];
	}

	return null;
};

/**
 * Parses a coordinates tuple.
 */
const parseCoordsTuple: Parser = coords => {
	if (!Array.isArray(coords) || coords.length < 1 || coords.length > 3) {
		return null;
	}

	if (coords.some(value => typeof value !== "number" || Number.isNaN(value))) {
		return null;
	}

	const [ x = 0, y = 0, z = 0 ] = coords;
	return [ x, y, z ];
};

export const parsers: Parser[] = [
	parseCoordsObject,
	parseCoordsTuple
];