/// <reference path="../_references" />

import {Inject} from './inject';
import {makeDecorator} from '../utils';

export const enum Restriction {
    Element,
    Attribute,
    Class,
    Comment
}

export interface ComponentOptions {

    selector:string;
    multiElement?:boolean;        
    priority?:number;
    terminal?:boolean;
    
    // Juntar com bind?
    scope?:boolean|{[member:string]:any};
    bindToController?:boolean;
    
    require?:string|string[];
    restrict?:Restriction|Restriction[];    
    
    // Que tal um enum com Contents e Element?
    transclude?:boolean|string;        
    
    compile?:any;
    
    // deprecated
    replace?:boolean;

}

export class ComponentAnnotation {

    constructor(options: ComponentOptions) {
        
    }

}

type ComponentDecorator = (options: ComponentOptions) => ClassDecorator;
export var Component = <ComponentDecorator> makeDecorator(ComponentAnnotation);

export function makeDirective(DirectiveClass:Function) {
    
    Inject(['$injector'], factory);
    function factory($injector: ng.auto.IInjectorService):ng.IDirective {
        
        return null;
        
    }
    
    return factory; 
    
}

/*

@Component({
    selector: 'card',
    bind: {
        title: '@'
    }
})
@View({
    template: '<div>I\' a card',
    templateNamespace: 'html',
    controllerAs: 'card',    
})
class Card {

}

*/