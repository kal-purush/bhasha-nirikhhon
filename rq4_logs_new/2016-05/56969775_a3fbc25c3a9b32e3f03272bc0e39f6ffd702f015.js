/*
 * @copyright Adam Benda, 2016
 */


/**
 * @class
 * @classdesc Overlay message: begin and end screen
 */

/**
 * 
 */
var Overlay = function () {
    /**
     * @type {HTMLDivElement}
     */
    this.screen = null;


};

Overlay.prototype.show = function (content) {
    document.getElementById('overlay').style.backgroundColor = 'rgba(0,0,0,0.3)';
    this.screen = document.createElement('div');
    this.screen.setAttribute('id', 'endScreen');
    this.screen.innerHTML = content;

    document.getElementById('overlay').appendChild(this.screen);
};

Overlay.prototype.hide = function () {
    document.getElementById('overlay').style.backgroundColor = 'transparent';
    if (this.screen !== null) {
        document.getElementById('overlay').removeChild(this.screen);
        this.screen = null;
    }
};

Overlay.prototype.showEndScreen = function (gamestate) {
    this.show("Dospěl jsi s Evelínou ke <strong>SCORE " + (Math.round(gamestate.getScore())) +
            "</strong>" + "<p><a href=\"#\" id=\"againA\">Chceš to zkusit znovu?</a></p>" +
            "<p>A už jsi evaluoval? Čím více dotazníků, tím lepší bonusy :)</p>");

    document.getElementById('againA').onclick = gamestate.restartAndStart.bind(gamestate);
};


Overlay.prototype.showBeginScreen = function (gamestate) {
    var html = "";
    html += "<h1>Kolosvist</h1>";
    html += "<a id=\"enterGameA\">Vstup do hry &gt;</a>";
    this.show(html);

    document.getElementById('enterGameA').onclick = gamestate.start.bind(gamestate);
};