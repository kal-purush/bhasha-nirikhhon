import { Renderer } from "./renderer";
export declare class CanvasRenderer extends Renderer {
    private _canvas;
    private _ctx;
    private _cachedBackground;
    constructor(el: HTMLElement, options: object);
    drawCircle(color: string, lineWidth: number, percent: number): void;
    protected drawScale(): void;
    clear(): void;
    draw(percent: number): void;
    get canvas(): HTMLCanvasElement;
    set canvas(value: HTMLCanvasElement);
    get ctx(): CanvasRenderingContext2D;
    set ctx(value: CanvasRenderingContext2D);
    get cachedBackground(): ImageData;
    set cachedBackground(value: ImageData);
}