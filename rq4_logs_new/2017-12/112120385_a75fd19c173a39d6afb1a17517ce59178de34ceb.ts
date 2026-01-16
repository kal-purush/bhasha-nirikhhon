import { Injectable } from '@angular/core';
import { Action, AnyAction } from 'redux';

@Injectable()
export class CanvasActions {
    static CHANGE_HEIGHT = 'pixelChange.changeHeight';
    static CHANGE_WIDTH = 'pixelChange.changeWidth';

    changeHeight(height: number): AnyAction {
        return {
            type: CanvasActions.CHANGE_HEIGHT,
            value: height
        };
    }
    
    changeWidth(width: number): AnyAction {
        return {
            type: CanvasActions.CHANGE_WIDTH,
            value: width
        };
    }
}
