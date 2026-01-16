/**
 * Created by jcabresos on 3/27/15.
 */
import kola = require('kola');
import signals = require('kola-signals');
import hooks = require('kola-hooks');
import PIXI = require('pixi.js');

export interface Kontext extends kola.Kontext {
    setSignal<T>(name: string, hook?: kola.Hook<T>): kola.SignalHook<T>;
    getSignal<T>(name: string): signals.Dispatcher<T>;
    setInstance<T>(name: string, factory: () => T): kola.KontextFactory<T>;
    getInstance<T>(name: string): T;
}


export class App extends kola.App<{container:PIXI.Container}> {
    sprite;
    listeners: signals.Listener<any>[] = [];
    container:PIXI.Container;

    onStart(): void {
        this.container = this.opts.container;
        var texture = PIXI.Texture.fromImage('images/cat.png');

        this.sprite = new PIXI.Sprite(texture);

        this.sprite.scale.x = 0.1;
        this.sprite.scale.y = 0.1 ;

        // move the sprite to the center of the screen
        this.sprite.position.x = Math.random() * this.container.getBounds().width;
        this.sprite.position.y = Math.random() * this.container.getBounds().height;

        console.log(this.container.width);

        this.container.addChild(this.sprite);
        this.listeners.push(this.kontext.getSignal('stage.render').listen(this.updateView, this));
    }

    updateView():void{
        //this.sprite.rotation += 0.05;
    }

    onStop(): void {
        this.listeners.forEach((listener: signals.Listener<any>) => {listener.unlisten()});
    }
}