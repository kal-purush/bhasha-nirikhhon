(function($) {


    Drupal.behaviors.edoweb_drupal_theme_searchbutton = {
    attach: function (context, settings) {

      replaceWithButton($('input #edit-query-0-submit'), 'batch-icons batch-icon-plus', context);

      function replaceWithButton(target, iconCss) {
        target.after('<button type="submit"><span class="' + iconCss + '"></span></button>'
      }
      
    }
  };
  
  
})(jQuery);