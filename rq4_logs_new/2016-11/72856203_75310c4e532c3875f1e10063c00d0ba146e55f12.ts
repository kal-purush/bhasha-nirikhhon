declare module 'simplify-geojson' {
    var simplifyGeojson: any;
    export = simplifyGeojson;
}

declare module 'togeojson' {
    var togeojson: {
        gpx(doc:Document): GeoJSON.FeatureCollection<GeoJSON.GeometryObject>;
    };
    export = togeojson;
}