import {makeDecorator, FunctionReturningSomething, setIfInterface} from '../utils';

export interface AnimationOptions {
    name: string;
}

// @internal
export class AnimationAnnotation {

    name: string = '';

    constructor(options: AnimationOptions) {
        setIfInterface(this, options);
    }

}

type cancelFunction = (isCancelled: boolean) => void;
export interface Filter {
    enter?: (element: ng.IAugmentedJQuery, done: Function) => cancelFunction;
    leave?: (element: ng.IAugmentedJQuery, done: Function) => cancelFunction;
    move?: (element: ng.IAugmentedJQuery, done: Function) => cancelFunction;
    addClass?: (element: ng.IAugmentedJQuery, className: string, done: Function) => cancelFunction;
    removeClass?: (element: ng.IAugmentedJQuery, className: string, done: Function) => cancelFunction;
}

type AnimationSignature = (options: AnimationOptions) => ClassDecorator;
export var Animation = <AnimationSignature> makeDecorator(AnimationAnnotation);