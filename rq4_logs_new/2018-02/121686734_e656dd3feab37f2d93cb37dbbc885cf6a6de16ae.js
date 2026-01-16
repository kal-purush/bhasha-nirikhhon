
function import_js(path, f) {
    var script = document.createElement('script');
    script.src = path;
    script.type = 'text/javascript';
    script.onload = f;
    document.getElementsByTagName('head')[0].appendChild(script);
}

function start() {
  $('head').append('<link rel="stylesheet" href="//code.jquery.com/ui/1.12.1/themes/base/jquery-ui.css" type="text/css" />');
	div = $(`
    <div id="xx-chat" class="ui-widget-content" style="z-index: 1234; position: absolute; background: transparent; border: 0; opacity: 0.4;">
      <p style="padding-top: 50px; background: purple; opacity: 0.1;"><p/>
      <iframe scrolling="yes" allowTransparency="true" id="xx-iframe" src="/embed${window.location.pathname}/chat?darkpopout"
          height="300" width="300"></iframe>
    </div>`);
  $(".video-player__container").append(div);
  $(div).draggable().resizable({alsoResize: "#xx-iframe"});
}

import_js("https://code.jquery.com/jquery-1.12.4.js", function() {
  import_js("https://code.jquery.com/ui/1.12.1/jquery-ui.js", start);
});