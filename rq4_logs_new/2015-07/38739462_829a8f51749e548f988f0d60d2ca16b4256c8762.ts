/// <reference path="typings/vendor/jquery/jquery.d.ts"/>
/// <reference path="typings/vendor/leaflet/leaflet.d.ts"/>

(function($) {
  var pluginName: string = 'leafletSearch';

  class LeafletSearch {
    _name: string = pluginName;
    _defaults: {

    };

    element: JQuery;
    settings: Object;

    constructor(element: JQuery, options: any) {
      this.element = element;
			this.settings = $.extend( {}, this._defaults, options );
      this.init();
    }

    init: Function = function() {

    }
  }

  $.fn[ pluginName ] = function ( options ) {
			return this.each(function() {
					if ( !$.data( this, "plugin_" + pluginName ) ) {
							$.data( this, "plugin_" + pluginName, new LeafletSearch( this, options ) );
					}
			});
	};

})(jQuery)