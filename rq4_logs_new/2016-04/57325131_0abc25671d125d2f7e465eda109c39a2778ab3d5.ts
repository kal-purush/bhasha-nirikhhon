/// <reference path="pixi/pixi.d.ts" />
declare module PIXI {

    export class Container extends DisplayObjectContainer implements IPixiRenderer
    {
        view: HTMLCanvasElement;
        render(stage: Stage): void;
        interactionManager: InteractionManager;
        getMousePosition(): Point;
        setBackgroundColor(backgroundColor: number): void;
    }

    export class loader
    {
        static add(name: string, path: string): loader;
        load(func: Function): loader;
    }
} 