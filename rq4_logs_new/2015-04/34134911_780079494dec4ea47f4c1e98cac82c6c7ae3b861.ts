/// <reference path="./gdal.d.ts" />
import gdal = require('gdal');

var p = new gdal.Polygon();

var ref = gdal.SpatialReference.fromEPSGA(26910);
console.dir(ref.getLinearUnits());
var ref4326 = gdal.SpatialReference.fromEPSG(4326);
console.log(ref4326.toProj4());

var transformation = new gdal.CoordinateTransformation(ref, ref4326);

var pt = transformation.transformPoint(0, 0);
console.dir(pt);


var wms = 'AUTO:42001,99,8888';
var refWMS = gdal.SpatialReference.fromWMSAUTO(wms);

var mpoly = gdal.Geometry.create(gdal.wkbMultiPolygon);
console.dir(mpoly);