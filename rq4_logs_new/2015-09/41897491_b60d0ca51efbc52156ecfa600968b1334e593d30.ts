/**
 * Created by rsandagon on 3/27/15.
 */
import kola = require('kola');
import signals = require('kola-signals');
import hooks = require('kola-hooks');
import PIXI = require('pixi.js');
import models = require('../models');
import bayawApp = require('../bayaw/app');

export interface Kontext extends kola.Kontext {
    setSignal<T>(name: string, hook?: kola.Hook<T>): kola.SignalHook<T>;
    getSignal<T>(name: string): signals.Dispatcher<T>;
    setInstance<T>(name: string, factory: () => T): kola.KontextFactory<T>;
    getInstance<T>(name: string): T;
}

export class App extends kola.App<{container:PIXI.Container}> {
    listeners: signals.Listener<any>[] = [];
    container:PIXI.Container;
    gameModel: models.GameModel;
    bayaw:bayawApp.App;

    ballotBoxes: PIXI.Sprite[] = [];

    onStart(): void {
        this.gameModel = <models.GameModel> this.kontext.getInstance('game.model');
        this.bayaw = <bayawApp.App> this.kontext.getInstance('player.bayaw');

        this.container = this.opts.container;
        
        var texture = PIXI.Texture.fromImage('images/ballotBox.png');

        for (var i = 0; i < 10; i++){
            var ballotBox = new PIXI.Sprite(texture);
            ballotBox.position.x = this.gameModel.width + (3 * Math.random() * this.gameModel.width);
            ballotBox.position.y = (Math.random() > 0.5) ? this.gameModel.height*0.25 : this.gameModel.height*0.75;

            TweenMax.to(ballotBox.position, 1, { y : (ballotBox.position.y + 20) , repeat:-1, yoyo:true});

            this.container.addChild(ballotBox);
            this.ballotBoxes.push(ballotBox);
        }
        
        this.listeners.push(this.kontext.getSignal('stage.render').listen(this.updateView, this));
    }

    updateView():void{
        if (this.gameModel.currentState == models.GameState.PLAYING) {

            for (var i = 0; i < 10; i++){
                var item = this.ballotBoxes[i];
                item.position.x -= models.GameSpeed.GROUND_SPEED;

                if(this.isIntersecting(item,this.bayaw.sprite)){
                    item.position.x = -150
                    //TweenMax.to(item.position, 1, { y : (ballotBox.position.y + 20) , repeat:-1, yoyo:true});
                }

                if (item.position.x < -150){
                   item.position.x = this.gameModel.width + (3 * Math.random() * this.gameModel.width);
                    item.position.y = (Math.random() > 0.5) ? this.gameModel.height*0.25 : this.gameModel.height*0.75;
                }
            }
        }
    }

    isIntersecting(r1, r2):boolean {
        return !(r2.position.x > (r1.position.x + r1.width) ||
            (r2.position.x + r2.width) < r1.position.x ||
            r2.position.y > (r1.position.y + r1.height) ||
            (r2.position.y + r2.height) < r1.position.y);
    }

    onStop(): void {
        //this.container.removeChild(this.floodMC);
        this.listeners.forEach((listener: signals.Listener<any>) => {listener.unlisten()});
    }
}