/// <reference path="leaflet.d.ts" />

declare module L {
	
	var ExtraMarkers: any;
	
	export interface ExtraMarkersOptions extends MarkerOptions {
		
		icon?: string;
		
		/**
		 * fa
		 * glyphicon
		 */
		prefix?: string;
		
		/**
		 * blue
		 * red
		 * orange-dark
		 * orange
		 * yellow
		 * blue-dark
		 * cyan
		 * purple
		 * violet
		 * pink
		 * green-dark
		 * green
		 * green-light 
		 * black
		 * white 
		 */ 
		markerColor?: string;
		
		/**
		 * circle
		 * square
		 * star
		 * penta
		 */	
		shape?: string;
		
		iconColor?: string;
		extraClasses?: string;
		number?: string;
	}

}