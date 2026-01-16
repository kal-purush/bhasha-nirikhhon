import WidgetTypes = require("./WidgetTypes");
import BaseWidgetCreator = require("./BaseWidgetCreator");
import BaseWidget = require("./BaseWidget");
import BoardTypes = require("./BoardTypes");

class BaseBoard {

    protected _canvas: HTMLElement;
    private _isMoving = false;
    private _sx = 0;
    private _sy = 0;
    private _ox = 0;
    private _oy = 0;
    private _scale: number = 1;
    private _widgets: BaseWidget[];
    private _eventListeners: { element: any; eventType: string; listner: EventListener }[];

    constructor(data: BoardTypes.IWidgetData[]) {
        this._canvas = this.getCanvas();
        this._eventListeners = [];
        this._addEventHandler(this._canvas, "mousedown", this._onMouseDown);
        this._addEventHandler(this._canvas, "mouseup", this._onMouseUp);
        this._addEventHandler(this._canvas, "mousemove", this._onMouseMove);
        this._addEventHandler(this._canvas, "wheel", this._onWheel);
        this._widgets = [];
        var widgetCreator = this.getWidgetCreator();
        data.forEach((widget) => {
            this._widgets.push(widgetCreator.createWidget(widget.type, this, widget));
        });
    }

    protected _addEventHandler(element: any, eventType: string, listner: EventListener) {
        var eventListner = listner.bind(this);
        element.addEventListener(eventType, eventListner);
        this._eventListeners.push({ element: element, eventType: eventType, listner: eventListner });
    }

    protected getCanvas(): HTMLElement {
        throw new Error("Abstact method");
    }

    protected getWidgetCreator(): BaseWidgetCreator {
        throw new Error("Abstact method");
    }

    public start(dx:number=0,dy:number=0,scale:number=1) {        
        this._scale = scale;
        this.draw(dx, dx);
    }

    public dispose() {
        this._eventListeners.forEach((eventListner) => {
            eventListner.element.removeEventListener(eventListner.eventType, eventListner.listner);
        })
    }

    private _onMouseDown(event: MouseEvent) {
        this._isMoving = true;
        this._sx = event.clientX;
        this._sy = event.clientY;

    }

    private _onMouseUp(event: MouseEvent) {
        this._isMoving = false;
    }

    private _onWheel(e: any) {
        var delta = e.deltaY || e.detail || e.wheelDelta;

        if (delta < 0) {
            if (this._scale >= 7.0) {
                return;
            }
            this._scale += 0.05;
        }
        else {
            if (Math.abs(this._scale-0.05) <= 0.01) {
                return;
            }
            this._scale -= 0.05;
        }

        this._ox = e.clientX;
        this._oy = e.clientY;
        this.draw(0, 0);
    }

    private _onMouseMove(event: MouseEvent) {
        if (this._isMoving) {
            this.draw(event.clientX - this._sx, event.clientY - this._sy);
            this._sx = event.clientX;
            this._sy = event.clientY;
        }
    }

    public draw(sx: number, sy: number) {
        this._widgets.forEach((widget) => {
            widget.draw(sx, sy, this._ox, this._oy, this._scale);
        });
    }

}
export =BaseBoard;
