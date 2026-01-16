if(typeof($add)=="undefined")var $add={version:{},auto:{disabled:false}};(function($){
  $add.version.Toggle = "6.0.1";
  $add.Toggle = function(selector, value, settings){
    var r = $(selector).map(function(i, el){
      var $el = $(el);
      var S = $.extend({}, settings, $el.data());

      if($el.attr("id")) S.id = $el.attr("id");
      if($el.attr("name")) S.name = $el.attr("name");
      if($el.attr("class")) S.class = $el.attr("class");
      if($el.attr("placeholder")) S.placeholder = $el.attr("placeholder");

      if($el.val()) var val = true;
      else var val = value;

      var o = new $add.Toggle.Obj(val, S);
      o.render($el, "replace");
      return o;
    });
    return (r.length==0)?null:(r.length==1)?r[0]:r;
  };
  $add.Toggle.Obj = Obj.create(function(value, settings){
    this.defSettings({
      id: false,
      name: false,
      placeholder: false,
      class: "",
      callback: function(){}
    });

    this.defMember("value", false);
    this.defMethod("toggle", function(){
      this.value = !this._value;
    });

    this.renderer = function(){
      var self = this;
      var $tf = $("<div class='addui-Toggle-field'></div>");
      var $t = $("<div class='addui-Toggle'><div class='addui-Toggle-handle'></div></div>").appendTo($tf);
      if(this._settings.class) $t.addClass(this._settings.class);
      if(this._settings.placeholder){
        $tf.css("display", "block");
        $ph = $("<div class='addui-Toggle-placeholder'>"+this._settings.placeholder+"</div>").appendTo($tf);
      }
      var $input = $("<input type='hidden' value='"+this.value+"' class='addui-Toggle-input' />").appendTo($tf);
      if(this._settings.id) $input.attr("id", this._settings.id);
      if(this._settings.name) $input.attr("name", this._settings.name);
      if(this._value){
        $tf.addClass("addui-Toggle-on");
      }

      $tf.on("click", function(){
        self.toggle();
      });

      return $tf;
    };
    this.refresher = function($el, changed){
      if(changed == "value"){
        if(this._value){
          $el.addClass("addui-Toggle-on");
        } else {
          $el.removeClass("addui-Toggle-on");
        }
        $el.find(".addui-Toggle-input").val(this._value);
      } else return this.renderer();
    };

    this.defMethod("init", function(value, settings){
      if(value) this.value = true;
      if(settings) this.settings = settings;
      this.on("setvalue", this._settings.callback);
    });
    this.init.apply(this, arguments);
  });
  $.fn.addToggle = function(value, settings){
    $add.Toggle(this, value, settings);
  };
  $add.auto.Toggle = function(){
    if(!$add.auto.disabled){
      $("input[data-addui=toggle]").addToggle();
    }
  };
})(jQuery);
$(function(){for(var k in $add.auto){if(typeof($add.auto[k])=="function"){$add.auto[k]();}}});