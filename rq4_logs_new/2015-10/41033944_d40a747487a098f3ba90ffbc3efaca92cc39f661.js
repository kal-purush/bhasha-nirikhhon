(function($) {
  Drupal.behaviors.comstack_pm_ui = {
    attach: function(context, settings) {
      if (typeof Drupal.settings.comstack_pm_ui.tooltips != 'undefined') {
        $('[data-toggle="tooltip"]', context).tooltip();
      }
    }
  };
})(jQuery);