 function main() {
        cartodb.createVis('carto-map', 'http://philakeyspots.cartodb.com/api/v2/viz/75f8c996-b86f-11e4-92ef-0e4fddd5de28/viz.json', {
            shareable: false,
            title: false,
            description: true,
            search: false,
            tiles_loader: true,
            fullscreen: false,
            legend: true,
            center_lat: 40,
            center_lon: -75.15,
            zoom: 12
        })
        .done(function(vis, layers) {
          // layer 0 is the base layer, layer 1 is cartodb layer
          // setInteraction is disabled by default
          layers[1].setInteraction(true);
          layers[1].on('featureOver', function(e, pos, latlng, data) {
            //cartodb.log.log(e, pos, latlng, data);
            search(data);
            
          });
          // you can get the native map to work with it
          var map = vis.getNativeMap();
          // now, perform any operations you need
          // map.setZoom(3);
          // map.panTo([50.5, 30.5]);
        })
        .error(function(err) {
          console.log(err);
        });
      }

  function search(data) {
    if (data.organization) {
      console.log(data);
      console.log
    }
  }
      window.onload = main;