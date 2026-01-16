// the semi-colon before the function invocation is a safety 
// net against concatenated scripts and/or other plugins 
// that are not closed properly.
;(function ( $, window, document, undefined ) {
  'use strict';

  var Shortcodes = vc.shortcodes;

  //
  // ATTS
  // -------------------------------------------------------------------------
  _.extend(vc.atts, {
    vc_efa_chosen:{
      parse:function ( param ) {
        var value = this.content().find('.wpb_vc_param_value[name=' + param.param_name + ']').val();
        return ( value ) ? value.join(',') : '';
      }
    },
  });

})( jQuery, window, document );