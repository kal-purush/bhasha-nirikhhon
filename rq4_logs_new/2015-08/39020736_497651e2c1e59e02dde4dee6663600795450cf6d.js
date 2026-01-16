/*
 * RA -> lng -> x
 * DEC -> lat -> y
 */

var LngLatDynamic = function(bounds){
    this.setBounds(bounds);
}
L.Projection.LngLatDynamic = function(bounds){
    return new LngLatDynamic(bounds);
}

LngLatDynamic.prototype.setBounds = function(bounds){
    if (bounds && bounds.decmax){
        bounds = L.bounds([bounds.ramin, bounds.decmin], [bounds.ramax, bounds.decmax]);
    }
    this.bounds = bounds || L.bounds([-180, -90], [180, 90]);   
    var xmin = this.bounds.min.x;
    var xmax = this.bounds.max.x;
    var ymin = this.bounds.min.y;
    var ymax = this.bounds.max.y;
    this.transformation = new L.Transformation((xmax-xmin)/256, xmin, (ymax-ymin)/256, ymin);
}

LngLatDynamic.prototype.project = function(latlng){
    var point = new L.Point(latlng.lng, latlng.lat);
    return this.transformation.untransform(point);
}

LngLatDynamic.prototype.unproject = function(point){
    point = this.transformation.transform(point);
    return new L.LatLng(point.y, point.x);
}



L.CRS.SimpleDynamic = function(bounds){
    return new SimpleDynamicCRS(bounds);
} 

var SimpleDynamicCRS = function(bounds){
    this.transformation = new L.Transformation(1, 0, 1, 0);
    this.projection = L.Projection.LngLatDynamic(bounds); 
    this.setBounds(bounds || L.bounds([-180, -90], [180, 90]));
}

SimpleDynamicCRS.prototype = L.extend(SimpleDynamicCRS.prototype, L.CRS.Simple, {infinity: false});

SimpleDynamicCRS.prototype.setBounds = function(bounds){
   this.projection.setBounds(bounds); 
}
