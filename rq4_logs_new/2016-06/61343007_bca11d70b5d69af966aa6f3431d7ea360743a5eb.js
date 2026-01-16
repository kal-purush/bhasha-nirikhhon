(function() {
  angular.module('unicornTracker')
    .service('initValsService', function() {
      this.initialMapView = [37.77416, -122.23252];
      this.latLonValidColor = {color: 'black'};
      this.latLonInvalidColor = {color: 'red'};
      this.mapboxAccessToken = 'pk.eyJ1Ijoic2FyYWgtYXQtcmVsb2xhIiwiYSI6ImI5YzY2ZjgxNDg5MDM5ZThmNTI5OWUxNTE4ZDA0OTlmIn0.gnTiJFzNaFOeSz6zHM67wg';
      this.mapStyleLayer = 'mapbox://styles/sarah-at-relola/ciow1xhy3002parm3itex8eem';
      this.maxMapZoom = 18;
      this.minMapZoom = 4;


    });
})();