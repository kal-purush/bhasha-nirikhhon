(function (Drupal, $) {
  window.dsm = function (val){
    console.log(val);
  };
  if (typeof (Drupal.grapesjs) == 'undefined') {
    Drupal.grapesjs = {
      instances : {},
      instance : null,
      uploadFormValues : {},
      assets : {},
      usedAssets : {},
      cssImageRegex : /url\((.*)\)/i,
      cssImageStyles : [
        'background',
        'background-image',
        'border',
        'border-image',
        'content',
        'cursor',
        'list-style',
        'list-style-image',
        'mask-image',
        'shape-outside',
        'mask-border',
        'mask-border-source'
      ]

    };
  }
  
  function clone (src) {
    return Object.assign({}, src);
  }
  
  Drupal.grapesjs.getAssets = function () {
  
    $.ajax({
      method: "POST",
      url: "/editor/grapesjs/assets",
      data: {},
      success: function (data, status, jqxhr) {
        if (status == 'success' && typeof (data) == 'object') {
          Drupal.grapesjs.addNewAssets(data.files);
        }
      }
    });
    
  };
  
  Drupal.grapesjs.addNewAssets = function (files) {
       
    var am = Drupal.grapesjs.instance.editor.AssetManager;
    
    var newAssets = [];
    for (var id in files) {
      if (typeof (Drupal.grapesjs.assets[files[id].src]) ==  'undefined') {
        var category = '';
        var a = {
          id: files[id].fid,
          category: category,
          src: files[id].src,
          type: 'image',
          entity: 'file',
        };
        newAssets[newAssets.length] = a;
        Drupal.grapesjs.assets[files[id].src] = a;
      }
    }

    if (newAssets.length > 0) {
      am.add(newAssets);
      
      // Need to make sure images add
      // data-entity-type="file" 
      // data-entity-uuid="64fe16f3-9450-4a08-a1f0-53e28ae4d8e3"
      
    }
    
  };

  Drupal.grapesjs.setUsedAsset = function (component, init) {
    
    if (typeof init == 'undefined' || init != true) {
      init = false;
    }
    
    if (typeof (component.attributes) == 'object')  {
      if (
        typeof(component.is) == 'function' &&
        component.is('image') && 
        typeof (Drupal.grapesjs.assets[component.attributes.src]) != 'undefined' &&
        typeof (Drupal.grapesjs.usedAssets[component.attributes.src]) == 'undefined' 
      ) {
        Drupal.grapesjs.usedAssets[component.attributes.src] = Drupal.grapesjs.assets[component.attributes.src];       
      }
      if (typeof (component.attributes.style) == 'string' && component.attributes.style != '') {
        Drupal.grapesjs.setUsedAssetsByString (component.attributes.style);       
      }
      else if (typeof (component.attributes.style) == 'object') {
        Drupal.grapesjs.setUsedAssetsByArray (component.attributes.style);
      }
      
      // Check the editor for css attributes on initialization only.
      if (init) {
        var editor = Drupal.grapesjs.instance.editor;
        var css = editor.CodeManager.getCode(component, 'css', {cssc: editor.CssComposer});
        var id = '#' + component.attributes.attributes.id;
        var regex = new RegExp(id + "{(.*)}", "i");
        if (css.match(regex)) {
          var start = id.length + 1;
          var end = css.length - (start + 1);
          var styleStr = css.substr(start, end);
          Drupal.grapesjs.setUsedAssetsByString (styleStr);
        }
      }

    } 
  }
  
  Drupal.grapesjs.setUsedAssetsByArray = function (styleArray) {
    $.each(styleArray, function(i,p) {
        if (Drupal.grapesjs.cssImageStyles.indexOf(i) && p.match(Drupal.grapesjs.cssImageRegex)) {
          p = p.trim();
          if (p != 'url(none)' && p != 'url()') {
            
            var s = p.substr(4);
            s = s.substr(0, s.length - 1);
            s = s.trim();
            
            var fchar = s.substr(0,1);
            var lchar = s.substr(-1);
            if ((fchar == '"' || fchar == "'") && fchar == lchar) {
              s = s.substr(1, s.length - 2);
            }
          
            if ( 
              typeof (Drupal.grapesjs.assets[s]) != 'undefined' &&
              typeof (Drupal.grapesjs.usedAssets[s]) == 'undefined'
            ) {
              Drupal.grapesjs.usedAssets[s] = Drupal.grapesjs.assets[s];
            }
          }
        }
     });
  };
  
  Drupal.grapesjs.setUsedAssetsByString = function (propStr) {
    var styles = {};
    var props = propStr.split(';');
    $.each(props, function(i,v) {
      var parts = v.split(':');
      if (typeof parts[1] == 'string') {
        styles[parts[0]] = parts[1].trim();
      }
    });
    Drupal.grapesjs.setUsedAssetsByArray(styles);
  };
  
  
  
  Drupal.grapesjs.initGrapes = function (id, components) {
    
    // Variables available for plugins to use in options.
    var vars = {
      navigator : navigator,
      window : window,
      Drupal : Drupal,
      '$' : $,
      'element' : '#' + id,
    };
          
    // 
    // Build the plugin options from the settings and from Javascript variables.
    //
    var pluginOpts = Drupal.settings.grapesjs.pluginOpts;
    $.each(Drupal.settings.grapesjs.pluginOptionsFromJs, function (pluginName, po) {
      if (Drupal.settings.grapesjs.plugins.includes(pluginName)) {
        if (typeof (pluginOpts[pluginName] != 'object')) {
          pluginOpts[pluginName] = {};
        }
        $.each(po, function (k, v) {
          if (typeof (v) == 'string') {
            var varFromJs = null;
            var split = v.split('.');
            if (split.length > 1) {
              var val = null;
              $.each(split, function (dotk, dotv) {
                if (val != null && typeof (val[dotv]) != 'undefined') {
                  val = val[dotv];
                }
                else if (val == null && typeof (vars[dotv]) != 'undefined') {
                  val = vars[dotv];
                }
              });
              varFromJs = val;
            }
            else {
              varFromJs = vars[v];
            }
            
//  To do
//  Split the string on . and/or [] to be able to access the variables inside of the variable.
// 
//  Example string to split
//    navigator.appName 
//
// End todo
            pluginOpts[pluginName][k] = varFromJs;

          }
        });
      }
    });

    // Set up the editor.
    var editor = grapesjs.init({
      autorender: 1,
//  forceClass: false,
 //     canvas: headElements,
      container : '#gjs',
      height: '100%',
      components: components,
      storageManager: {
        type: 'textarea',
      },
      plugins: Drupal.settings.grapesjs.plugins,
      pluginsOpts: pluginOpts,
      assetManager: {
        upload: '/editor/grapesjs/upload',
        uploadName: 'files',
        params: Drupal.grapesjs.uploadFormValues
      }

    });
    
/*
    $.each(headElements, function(type, els){
      if (typeof (els) == 'object') {
        $.each(els, function (i, el){
          if (typeof (els) == 'object' && el.innerHTML != '' && el.tagName == 'STYLE') {
            var styles = el.innerHTML;
            if (typeof (styles) == 'string') {
              var importCSS = /@import url\("(.*)"\);/gm;
              var cssScripts = styles.match(importCSS);
              if (typeof(cssScripts) == 'object' && cssScripts != null) {
                $.each(cssScripts, function (i, v) {
                  if (typeof (v) == 'string') {
                    var cssLink = v.substr(13, (v.length - 16));
                    editor.getComponents().add(el.outerHTML)
                  }
                });
              }
              styles = styles.replace(importCSS, '');
              if (styles.trim() != '') {
                console.log(el);
  //              editor.getComponents().add('<style>' + styles.trim() + '</style>');
              }
            }
          }
          else {
            editor.getComponents().add(el.outerHTML);
          }
        });
      }
    }); */
     
    editor.on('load', function(){
      var els = editor.DomComponents.getWrapper().find('*');
      $.each(els, function (i, el){
        Drupal.grapesjs.setUsedAsset (el,true);
      });
    });
 
   
    editor.on('component:add', (component) => {
      Drupal.grapesjs.setUsedAsset (component);
    });
    
    editor.on('component:update', (component) => {
      Drupal.grapesjs.setUsedAsset (component);
    });
    
    /*
    editor.on('asset:remove', (asset) => {
      console.log(asset);
    });
    editor.on('asset:upload:start', () => {
      console.log('start');
    });
    editor.on('asset:upload:end', () => {
      console.log('end');
    });
    editor.on('asset:upload:error', (error) => {
      console.log('error',error);
    });*/
    editor.on('asset:upload:response', (response) => {

      // Update the build id.
//      for (var i = 0; i<response.length;i++) {
//    if (response[i].command == 'update_build_id') {
//          uploadFormVals.form_build_id = response[i].new;
//        }
//      }
      //
      // Don't know how to modify drupal's response so we are
      // going to get the results ourselves.
      //            
     // Drupal.grapesjs.getAssets();            
      Drupal.grapesjs.addNewAssets(response.files);   
    });
    
    // Prevent the shortcut to save function for web browsers.
    var iframe = $('#gjs iframe');
    if (iframe.length > 0) {
          
      var iframeDoc = $(iframe)[0].contentDocument;
      $.each([document, iframeDoc],function( i, val ) {
        $(val).bind('keydown keyup keypress', function(e) {
          if (e.which == 83 && (navigator.platform.match("Mac") ? e.metaKey : e.ctrlKey)) {
            e.preventDefault();
            e.stopPropagation();
            return false;
          }
          else if (e.keyCode == 83 && (navigator.platform.match("Mac") ? e.metaKey : e.ctrlKey)) {
            e.preventDefault();
            e.stopPropagation();
            return false;
          }
        });
      });
      
      // Add in any extra stylesheets, meta tags, javascripts.
      $.each(Drupal.grapesjs.instance.head.meta, function(i, val){
        iframeDoc.head.appendChild(val);
      });
      $.each(Drupal.grapesjs.instance.head.css, function(i, val){
        iframeDoc.head.appendChild(val);
      });
      $.each(Drupal.grapesjs.instance.head.styles, function(i, val){
        iframeDoc.head.appendChild(val);
      });
      $.each(Drupal.grapesjs.instance.head.scripts, function(i, val){
        iframeDoc.head.appendChild(val);
      });
      
      var rr = document.createElement('script');
      rr.src = "/sites/all/modules/custom/grapesjs/js/responsive-resizing.js";
      iframeDoc.head.appendChild(rr);
      
      var s = document.createElement('style');
      s.innerHTML = '*[data-highlightable="1"]{min-height: 20px;min-width: 10px;}.gjs-edit-zone{border:2px dashed orange;margin: 0 -2px;}';
      iframeDoc.head.appendChild(s);
      
      Drupal.grapesjs.instance.iframe = iframeDoc;
      Drupal.grapesjs.instance.iframeWindow = $(iframe)[0].contentWindow ? $(iframe)[0].contentWindow : iframeDoc.defaultView;

    }
     
    Drupal.grapesjs.instance.editor = editor;
    
    
    // Asset Manager must be called here and not the plugin.
    Drupal.grapesjs.addNewAssets(Drupal.settings.grapesjs.assets);    
    
  };
  
  Drupal.grapesjs.getFullPageHtml = function (id) {
    
    Drupal.grapesjs.instance = {
      name : id,
      editor : null,
      head : null
    };
    var defaultHtml = $('#' + id).val();
    var url = Drupal.settings.grapesjs.url;
    
    // Set the default head styles to prevent errors when the url is not set.
    var head = {
      css: [],
      styles: [],
      scripts: [],
      meta: []
    };
    Drupal.grapesjs.instance.head = head;
    
    // Return just the default value if the url can't be found.
    if (url == '') {
      Drupal.grapesjs.initGrapes (id, defaultHtml);
      return;
    } 

    $.ajax({
      method: "GET",
      url: url,
      success: function (data, status, jqxhr) {
        if (status == 'success' && typeof (data) == 'string') {
        
          var doc = document.createElement('html');
          doc.innerHTML = data;
          
          var headEl = $('head', doc)[0];
          var bodyEl = $('body', doc)[0];

          var head = {
            css: [],
            styles: [],
            scripts: [],
            meta: []
          }; 

          $('link', headEl).each(function (i, val) {
            head.css.push(val);              
          });
          $('style', headEl).each(function (i, val) {
            head.styles.push(val);
          });
          $('script', headEl).each(function (i, val) {
            head.scripts.push(val);
          });
          $('meta', headEl).each(function (i, val) {
            head.meta.push(val);
          });
          
          Drupal.grapesjs.instance.head = head;

          // Add the grapesjs attributes to prevent editing to all the items.
          var gjsAttr = [
            'editable',
            'draggable',
            'droppable',
            'highlightable',
            'selectable',
            'removable',
            'badgable',
            'stylable',
            'copyable',
            'layerable',
            'hoverable',
          ];
          $.each(gjsAttr, function(i,v){
            $(bodyEl).find('*').attr('data-gjs-' + v, 'false');
          }); 
          
          // Get the html and replace the element in the body.  
          var f = $('#' + id).attr('data-field-name');
          var d = $('#' + id).attr('data-field-delta');     
          Drupal.grapesjs.instance.selector = '.grapesjs-editable-field-item';     
          if (typeof (f) == 'undefined' || typeof (d) == 'undefined') {
            Drupal.grapesjs.initGrapes (id, defaultHtml);
            return;
          }  
          Drupal.grapesjs.instance.field = f; 
          Drupal.grapesjs.instance.delta = d;  
          Drupal.grapesjs.instance.selector = '.grapesjs-editable-field-item[data-grapesjs-field="' + f + '"][data-grapesjs-delta="' + d + '"]';
          $(bodyEl).find(Drupal.grapesjs.instance.selector).removeAttr('data-gjs-editable').removeAttr('data-gjs-droppable').wrap('<div class="gjs-edit-zone"></div>');
          $.each(gjsAttr, function(i,v){
            $(bodyEl).find('.gjs-edit-zone').attr('data-gjs-' + v, 'false');
          }); 
          
          $(bodyEl).find(Drupal.grapesjs.instance.selector).html(defaultHtml);

          // Get the new body html.
          var body = $(bodyEl).html();
          Drupal.grapesjs.initGrapes (id, body);
//        Drupal.grapesjs.initGrapes (id, defaultHtml)

        }    
      },
      error: function (jqXHR, textStatus, errorThrown) {
        Drupal.grapesjs.initGrapes (id, defaultHtml)
      }
    });

  };
  
  Drupal.grapesjs.init = function (id) {

    var settings = Drupal.settings.grapesjs;
    $("#" + id).next(".grippie").css("display", "none");
    $("#" + id).addClass("grapesjs-processed");
        
    if ($('#gjs').length == 0) {
        
      $('body').append('<div id="gjs"></div>').promise().done(function(){          

        // Hide the textarea & grapes container.
        $('#gjs').hide();

        // Add a button in its place.
        $('#' + id).after('<div class="gjs-button-container" style="display: flex;align-items: center;justify-content: center;text-align: center;border: 1px solid #d1d1d1; height: 250px;"><div class="gjs-open button">Open Editor</div></div>').promise().done(function(){
          $('.gjs-open').click(function(){
            $('#gjs').fadeIn().focus();
            $('body > *:not("#gjs")').addClass('not-gjs');
/*            if (
              typeof Drupal.grapesjs.instance.iframeWindow != 'undefined' &&
              typeof Drupal.grapesjs.instance.iframeWindow.responsiveResizeInit != 'undefined'
            ) {
              Drupal.grapesjs.instance.iframeWindow.responsiveResizeInit();
              Drupal.grapesjs.instance.iframeWindow.responsiveResizeResize();
            }*/
          });
          Drupal.grapesjs.getFullPageHtml (id);
        });
         
      });

    }
    // Fallback this should not happen.
    else {
      $('body > *:not("#gjs")').removeClass('not-gjs');
      $('#gjs').hide();
      $('.gjs-button-container').show();
      Drupal.grapesjs.getFullPageHtml (id);
    }

  };
  
  Drupal.grapesjs.on = function (id) {
    
    if (typeof (id) == 'undefined' || id.length == 0 || $("#" + id).length == 0) {
      return;
    }
    if (typeof (Drupal.grapesjs.instances[id]) != 'undefined') {
      return;
    }
    Drupal.grapesjs.init(id);

  };

  Drupal.grapesjs.off = function (id) {
    if (!Drupal.grapesjs.instances || typeof(Drupal.grapesjs.instances[id]) == 'undefined') {
      return;
    }
    if (Drupal.grapesjs.instance && Drupal.grapesjs.instance.name == id) {
      Drupal.grapesjs.instance = null;
    }
//
// Need to change the get data to the actual call.
// Need to change destroy to actual call.
//    
    $("#" + id).val(grapesjs.instances[id].getData());
    Drupal.grapesjs.instances[id].destroy(true);
    delete Drupal.grapesjs.instances[id];
    $("#" + id).next(".grippie").css("display", "block");
    
  };
  
  Drupal.grapesjs.attach = function (context) {

    // Make sure the textarea behavior is run first, to get a correctly sized grippie
    if (Drupal.behaviors.textarea && Drupal.behaviors.textarea.attach) {
      Drupal.behaviors.textarea.attach(context);
    }
    
    $(context).find("textarea.grapesjs-mod:not(.grapesjs-processed)").each(function () {
     
      var id = $(this).attr("id");
  
      // Exit if the summary.
      if (id.substr (-7) == 'summary') {
        return;
      }
     
      // Shutoff the editor if already on.
      if (Drupal.grapesjs.instances && typeof(Drupal.grapesjs.instances[id]) != 'undefined'){
        Drupal.grapesjs.off(id);
      }

      // Get the select setting
      var selFormat = $("#" + id.substr(0, id.lastIndexOf("-")) + "-format--2");
      
      // Turn on the editor if it is the input type.
      if (typeof(Drupal.settings.grapesjs.input_formats[$(selFormat).val()]) != 'undefined'){
        Drupal.grapesjs.on(id);
      }
      
      if (selFormat && selFormat.not('.grapesjs-processed')) {
        selFormat.addClass('grapesjs-processed').change(function() {
          if (Drupal.grapesjs.instances && typeof(Drupal.grapesjs.instances[id]) != 'undefined') {
// Switch to correct call
            $('#'+id).val(Drupal.grapesjs.instances[id].getData());
          }
          Drupal.grapesjs.off(id);
          if (typeof(Drupal.settings.grapesjs.input_formats[$(this).val()]) != 'undefined'){
            Drupal.grapesjs.on(id);
          }
          else {
            Drupal.grapesjs.off(id);
            Drupal.behaviors.grapesjs.detach(context, {}, 'unload');
          }
        });
      }
    });
  };  
  
  Drupal.grapesjs.getUploadForm = function (context) {
    $.ajax({
       method: "POST",
       url: "/editor/grapesjs/get-upload-form",
       data: {
        'fid' : 'upload',
       },
       dataType: 'json',
       success: function (data, status, jqxhr) {

        if (status == 'success' && typeof (data) == 'object') {
          var uploadFormVals = {};
          $('body').append('<div class="grapesjs-upload-form-container">' + data.form + '</div>').promise().done(function(){
            var vals = $('.grapesjs-upload-form-container form').serializeArray();
            for (var i in vals) {
              if (typeof (vals[i]) == 'object') {
                uploadFormVals[vals[i].name] = vals[i].value;
              }
              
            }
            
            // Note:
            //  ajax_html_ids are necessary to post so that the form generation doesn't assign
            //  used html ids to form elements
            //
            var ajax_html_ids = [];
            $('[id]').each(function () {
              ajax_html_ids.push(this.id);
            });
           
            // Add special data needed to trick it into believing it was triggered by the upload button on the form.
            uploadFormVals._triggering_element_name = 'op';
            uploadFormVals._triggering_element_value = 'Submit';
            uploadFormVals['ajax_html_ids[]'] = ajax_html_ids;
            uploadFormVals['ajax_page_state[theme]'] = Drupal.settings.grapesjs.theme;
            uploadFormVals['ajax_page_state[theme_token]'] = ''; // Not needed because we are viewing the default theme.
            uploadFormVals['ajax_iframe_upload'] = 1;
            
            // Add this to our settings.
            Drupal.grapesjs.uploadFormValues = uploadFormVals;

            // Remove the form once we have the values.
            $('.grapesjs-upload-form-container').remove();

            // Now call the regular attach.
            Drupal.grapesjs.attach (context);

          });
        }
       },
       error: function () {
         alert ('There was a problem initializing the upload form. You may be unable to upload images using the GrapesJs editor.');
         Drupal.grapesjs.attach (context);
       }
    });

  }

  var gjseditor = {
    attach: function (context) {

//
// To do!!!!
//
// Fix and do this the right way somehow.
//          
// Currently we are loading a form that contains an upload field.
// Stealing its values, so that we can submit to it from the GrapesJs' Asset Manager
// and then entering them as params.  Then we are manually loading new assets using
// the function getAssets();
//      
// Then we could call Drupal.grapesjs.attach (context); directly from here.
//  
      Drupal.grapesjs.getUploadForm (context);
    },
    detach : function (context, settings, trigger) {   
      
      $(context).find("textarea.grapesjs-mod.grapesjs-processed").each(function () {
        var id = $(this).attr("id");
        if (trigger != 'serialize') {
          Drupal.grapesjs.off(id);
          $(this).removeClass('grapesjs-processed');
        }
      });
      
      
      $('body > *:not("#gjs")').removeClass('not-gjs');

      // Remove so new elements can be added via the other editors.
      $('#gjs').remove();
      $('.gjs-button-container').remove();
      
    }
  };

  Drupal.behaviors.grapesjs = gjseditor;
  
  // Support CTools detach event.
  $(document).bind('CToolsDetachBehaviors', function(event, context) {
    Drupal.behaviors.grapesjs.detach(context, {}, 'unload');
  });
  
})(Drupal, jQuery);