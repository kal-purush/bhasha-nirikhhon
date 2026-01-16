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


export class App extends kola.App<HTMLElement> {
    cat;
    renderer;

    initialize(kontext: Kontext, opts?: HTMLElement): void {

        this.renderer = PIXI.autoDetectRenderer(600, 800,{backgroundColor : 0x1099bb});
        this.opts.appendChild(this.renderer.view);

// create the root of the scene graph
        var stage = new PIXI.Container();

// create a texture from an image path
        var texture = PIXI.Texture.fromImage('images/cat.png');

// create a new Sprite using the texture
        this.cat = new PIXI.Sprite(texture);

// center the sprite's anchor point
        this.cat.anchor.x = 0.5;
        this.cat.anchor.y = 0.5;

        this.cat.scale.x = 0.3;
        this.cat.scale.y = 0.3;

// move the sprite to the center of the screen
        this.cat.position.x = 200;
        this.cat.position.y = 150;

        stage.addChild(this.cat);

// start animating
        requestAnimationFrame(this.catMove.bind(this));

    }

    catMove(timestamp):void {
        requestAnimationFrame(this.catMove.bind(this));
        // just for fun, let's rotate mr rabbit a little
        this.cat.rotation += 0.1;

        // render the container
        this.renderer.render(this.cat);
    }

    onStart(): void {

    }


    onStop(): void {
    }
}