/**
 * Created by rsandagon on 3/27/15.
 */
import kola = require('kola');
import signals = require('kola-signals');
import hooks = require('kola-hooks');
import PIXI = require('pixi.js');
import models = require('../models');

export interface Kontext extends kola.Kontext {
    setSignal<T>(name: string, hook?: kola.Hook<T>): kola.SignalHook<T>;
    getSignal<T>(name: string): signals.Dispatcher<T>;
    setInstance<T>(name: string, factory: () => T): kola.KontextFactory<T>;
    getInstance<T>(name: string): T;
}


export class App extends kola.App<{container:PIXI.Container}> {
    bg;
    fg;
    listeners: signals.Listener<any>[] = [];
    container:PIXI.Container;

    onStart(): void {
        var gameModel:models.GameModel = <models.GameModel> this.kontext.getInstance('game.model');

        this.container = this.opts.container;
        var texture1 = PIXI.Texture.fromImage('images/background.png');
        var texture2 = PIXI.Texture.fromImage('images/foreground.png');

        this.bg = new PIXI.extras.TilingSprite(texture1, gameModel.width, gameModel.height);
        this.fg = new PIXI.extras.TilingSprite(texture2, gameModel.width, gameModel.foregroundHeight);

        this.bg.position.x = 0;
        this.bg.position.y = 0;

        this.fg.position.x = 0;
        this.fg.position.y = 100;

        this.container.addChild(this.bg);
        this.container.addChild(this.fg);
        this.listeners.push(this.kontext.getSignal('stage.render').listen(this.updateView, this));
    }

    updateView():void{
        this.bg.tilePosition.x -= 0.1;
        this.fg.tilePosition.x -= 0.5;
    }

    onStop(): void {
        this.container.removeChild(this.bg);
        this.container.removeChild(this.fg);
        this.listeners.forEach((listener: signals.Listener<any>) => {listener.unlisten()});
    }
}