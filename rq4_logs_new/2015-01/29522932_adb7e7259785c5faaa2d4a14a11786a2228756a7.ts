/// <reference path="./../../bower_components/DefinitelyTyped/angularjs/angular.d.ts" />

/// <reference path="./../references/generic.d.ts" />

'use strict';

angular.module('mobileMasterApp').service('geocodingService', function (
	$http: ng.IHttpService) {

	var mapboxPublicToken = "pk.eyJ1IjoiYXB1bHRpZXIiLCJhIjoib25CYXRvNCJ9.0oA1cLtUH_V32zw6t2slkg";
	var mapboxEndpoint = "https://api.tiles.mapbox.com/v4/geocode/mapbox.places/";

	this.forward = (address: string, callback: (geojson: any) => void) => {
		$http.get(mapboxEndpoint + encodeURIComponent(address) + ".json?access_token=" + mapboxPublicToken)
			.success(callback)
			.error(() => callback(null));
	};
});