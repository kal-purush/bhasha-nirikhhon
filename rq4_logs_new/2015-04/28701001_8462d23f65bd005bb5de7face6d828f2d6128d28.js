"user strict"

Polymer( 'leaflet-hash-control', {

  created : function () {
  },

  containerChanged : function () {
    if ( this.container ) {
      new L.Hash( this.container );
    }
  }

} );