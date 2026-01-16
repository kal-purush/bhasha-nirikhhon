import {Illusion} from './illusion';
import {Options} from './options';

export interface IIllusion {
    new (options:Options, svg?:any):Illusion;
}