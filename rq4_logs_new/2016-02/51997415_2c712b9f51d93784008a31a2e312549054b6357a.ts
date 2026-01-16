'use strict';

export interface IGrid {

    //get width():number;
    //get height():number;

    set(first:number, second:number, third?:number):void;
    get(first:number, second?:number):number;
}