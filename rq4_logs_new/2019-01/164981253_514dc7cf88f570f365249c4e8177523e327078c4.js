/* WaterMark, (c) https://github.com/junliepigyao/vue-watermark
 * @license MIT */

;
(function (root, factory) {

  if (typeof define === 'function' && define.amd) {
    define(factory);
  } else if (typeof exports === 'object') {
    module.exports = factory();
  } else {
    root.WaterMark = factory();
  }

})(this, function () {
  var WaterMark = {};

  WaterMark.version = '1.0.0';

  var Settings = WaterMark.settings = {
    user: '',
    company: '',
    time: true,
    watermark_x: 20, //水印起始位置x轴坐标
    watermark_y: 20, //水印起始位置Y轴坐标
    watermark_rows: 20, //水印行数
    watermark_cols: 20, //水印列数
    watermark_x_space: 100, //水印x轴间隔
    watermark_y_space: 50, //水印y轴间隔
    watermark_color: '#aaa', //水印字体颜色
    watermark_alpha: 0.4, //水印透明度
    watermark_fontsize: '15px', //水印字体大小
    watermark_font: '微软雅黑', //水印字体
    watermark_width: 210, //水印宽度
    watermark_height: 80, //水印长度
    watermark_angle: 15 //水印倾斜度数
  };

  /**
   * Updates configuration.
   *
   *     WaterMark.configure({
   *       user: 'Jeffery Gou.'
   *     });
   */

  WaterMark.configure = function (options) {
    var key, value;
    for (key in options) {
      value = options[key];
      if (value !== undefined && options.hasOwnProperty(key)) Settings[key] = value;
    }

    // debugger;
    //采用配置项替换默认值，作用类似jquery.extend
    if (arguments.length === 1 && typeof arguments[0] === "object") {
      var src = arguments[0] || {};
      for (key in src) {
        if (src[key] && Settings[key] && src[key] === Settings[key])
          continue;
        else if (src[key])
          Settings[key] = src[key];
      }
    }

    var oTemp = document.createDocumentFragment();

    //获取页面最大宽度
    var page_width = Math.max(document.body.scrollWidth, document.body.clientWidth);
    var cutWidth = page_width * 0.0150;
    var page_width = page_width - cutWidth;
    //获取页面最大高度
    var page_height = Math.max(document.body.scrollHeight, document.body.clientHeight) + 450;
    // var page_height = document.body.scrollHeight+document.body.scrollTop;
    //如果将水印列数设置为0，或水印列数设置过大，超过页面最大宽度，则重新计算水印列数和水印x轴间隔
    if (Settings.watermark_cols == 0 || (parseInt(Settings.watermark_x + Settings.watermark_width * Settings.watermark_cols + Settings.watermark_x_space * (Settings.watermark_cols - 1)) > page_width)) {
      Settings.watermark_cols = parseInt((page_width - Settings.watermark_x + Settings.watermark_x_space) / (Settings.watermark_width + Settings.watermark_x_space));
      Settings.watermark_x_space = parseInt((page_width - Settings.watermark_x - Settings.watermark_width * Settings.watermark_cols) / (Settings.watermark_cols - 1));
    }
    //如果将水印行数设置为0，或水印行数设置过大，超过页面最大长度，则重新计算水印行数和水印y轴间隔
    if (Settings.watermark_rows == 0 || (parseInt(Settings.watermark_y + Settings.watermark_height * Settings.watermark_rows + Settings.watermark_y_space * (Settings.watermark_rows - 1)) > page_height)) {
      Settings.watermark_rows = parseInt((Settings.watermark_y_space + page_height - Settings.watermark_y) / (Settings.watermark_height + Settings.watermark_y_space));
      Settings.watermark_y_space = parseInt(((page_height - Settings.watermark_y) - Settings.watermark_height * Settings.watermark_rows) / (Settings.watermark_rows - 1));
    }
    var x;
    var y;
    for (var i = 0; i < Settings.watermark_rows; i++) {
      y = Settings.watermark_y + (Settings.watermark_y_space + Settings.watermark_height) * i;
      for (var j = 0; j < Settings.watermark_cols; j++) {
        x = Settings.watermark_x + (Settings.watermark_width + Settings.watermark_x_space) * j;

        var mask_div = document.createElement('div');
        mask_div.id = 'mask_div' + i + j;
        mask_div.className = 'mask_div';
        var watermarkText = Settings.company + Settings.user;
        mask_div.appendChild(document.createTextNode(watermarkText));
        if (Settings.time) {
          // watermarkText = watermarkText +"\n"+ CurentTime();
          var time_div = document.createElement('div');
          time_div.appendChild(document.createTextNode(CurentTime()));
          mask_div.appendChild(time_div);
        }
        //设置水印div倾斜显示
        mask_div.style.webkitTransform = "rotate(-" + Settings.watermark_angle + "deg)";
        mask_div.style.MozTransform = "rotate(-" + Settings.watermark_angle + "deg)";
        mask_div.style.msTransform = "rotate(-" + Settings.watermark_angle + "deg)";
        mask_div.style.OTransform = "rotate(-" + Settings.watermark_angle + "deg)";
        mask_div.style.transform = "rotate(-" + Settings.watermark_angle + "deg)";
        mask_div.style.visibility = "";
        mask_div.style.position = "absolute";
        mask_div.style.left = x + 'px';
        mask_div.style.top = y + 'px';
        mask_div.style.overflow = "hidden";
        mask_div.style.zIndex = "9999";
        mask_div.style.pointerEvents = 'none'; //pointer-events:none  让水印不遮挡页面的点击事件
        mask_div.style.opacity = Settings.watermark_alpha;
        mask_div.style.fontSize = Settings.watermark_fontsize;
        mask_div.style.fontFamily = Settings.watermark_font;
        mask_div.style.color = Settings.watermark_color;
        mask_div.style.textAlign = "center";
        mask_div.style.width = Settings.watermark_width + 'px';
        mask_div.style.height = Settings.watermark_height + 'px';
        mask_div.style.display = "block";
        oTemp.appendChild(mask_div);
      };
    };
    document.body.appendChild(oTemp);
  };

  function CurentTime() {
    var now = new Date();

    var year = now.getFullYear(); //年
    var month = now.getMonth() + 1; //月
    var day = now.getDate(); //日

    var hh = now.getHours(); //时
    var mm = now.getMinutes(); //分

    var clock = year + "-";

    if (month < 10)
      clock += "0";

    clock += month + "-";

    if (day < 10)
      clock += "0";

    clock += day + " ";

    if (hh < 10)
      clock += "0";

    clock += hh + ":";
    if (mm < 10) clock += '0';
    clock += mm;
    return (clock);
  }

  return WaterMark;
});