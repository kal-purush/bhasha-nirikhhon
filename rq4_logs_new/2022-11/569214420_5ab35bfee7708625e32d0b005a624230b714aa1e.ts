import { isObject } from "../utils";
import type { Coords, CoordsPolar, CoordsTuple } from "../types";

export const isCoords = (input: unknown): input is Coords => {
	if (!isObject(input)) {
		return false;
	}

	return ([ "x", "y", "z" ]
		.some(key => key in input));
};

export const isCoordsPolar = (input: unknown): input is CoordsPolar => {
	if (!isObject(input)) {
		return false;
	}

	return [ "phi", "theta" ]
		.some(key => key in input);
};

export const isCoordsTuple = (input: unknown): input is CoordsTuple => {
	if (!Array.isArray(input) || input.length < 1 || input.length > 3) {
		return false;
	}

	return input
		.every(item => typeof item === "number" && !Number.isNaN(item));
};