import {RenderData, RenderAction} from "./render-base";
import {RenderMethods, RenderType} from "./render-types";
/**
 * Created by Daniel Fallon on 12/30/2016.
 */

export interface LineData extends RenderData {
    startX: number,
    startY: number,
    endX: number,
    endY: number,

}

function renderLine(canvas):void{
    canvas.beginPath();
    canvas.moveTo(this.startX,this.startY);
    canvas.lineTo(this.endX,this.endY);
    canvas.stroke();
    canvas.closePath()
}

RenderMethods.register(RenderType.line,renderLine);