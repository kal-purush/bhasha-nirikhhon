/// <reference path="typings/pixi/pixi.d.ts" />
/// <reference path="typings/custom.d.ts" />

declare var Stage: PIXI.Container;

// Declare a global variable for our sprite so that the animate function can access it.
var background: PIXI.DisplayObject = null;

module PocketParryModule {
    'use strict';

    export class Background implements ISprite {

        private id: string = 'bg';
        private path: string = 'Assets/bg.gif';

        draw = () => {
            // load the texture we need
            PIXI.loader.add(this.id, this.path).load(function (loader, resources) {
            
                // This creates a texture from a 'bg.gif' image.
                var background = new PIXI.Sprite(resources.bg.texture);

                // Setup the position and scale of the background
                background.position.x = 0;
                background.position.y = 0;

                background.scale.x = 2;
                background.scale.y = 2;

                // Add the background to the scene we are building.
                Stage.addChild(background);

                // kick off the animation loop (defined below)
                //animate();
            });
        }
    } 
}

// instantiate the class and draw
new PocketParryModule.Background().draw();