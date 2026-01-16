/*global
    window, document
 */
(function (exports) {
    "use strict";

    function getBaseUrl() {
        var scriptUrl = document.scripts[document.scripts.length - 1].src;
        return scriptUrl.substring(0, scriptUrl.lastIndexOf('/') + 1);
    }

    function initNainwakletButton(id, nainwakletPath) {
        var button = document.getElementById(id),
            href = "javascript:(function(){var%20s=document.createElement('script');s.setAttribute('src','"
                    + baseUrl
                    + (nainwakletPath || "nainwaklet.js")
                    + "');document.getElementsByTagName('head')[0].appendChild(s);})();";
        button.setAttribute("href", href);
    }

    var baseUrl = getBaseUrl();
    exports.initNainwakletButton = initNainwakletButton;
}(window));