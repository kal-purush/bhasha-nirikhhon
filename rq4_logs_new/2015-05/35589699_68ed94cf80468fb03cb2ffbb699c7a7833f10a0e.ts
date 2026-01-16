/*
@DirectiveComponent({
    selector: 'card'
})
@DirectiveView({    
    controllerAs: 'card',                       // optional
    template: '<div>{{ card.content }}</div>,   // either
    templateUrl: 'card.html',                   // or
    templateFactory: templateFactory,           // or
    templateUrlFactory: templateUrlFactory,
    templateNamespace: TemplateNamespace.HTML   // optional
})
class Card {
    content = "I'm a card";    
}
*/

import {makeDecorator, FunctionReturningString} from '../utils';
import {ViewOptions, ViewAnnotation} from './view';

export const enum TemplateNamespace {
    HTML,
    SVG,
    MathML
}

export interface DirectiveViewOptions extends ViewOptions {

    templateNamespace?: string;
    
    // custom
    templateFactory?: FunctionReturningString;
    templateUrlFactory?: FunctionReturningString;

}

export class DirectiveViewAnnotation extends ViewAnnotation {

    templateNamespace: string; // TemplateNamespace.HTML
    templateFactory: FunctionReturningString;
    templateUrlFactory: FunctionReturningString;

    constructor(options: DirectiveViewOptions) {
        super(options);
        this.templateNamespace = options.templateNamespace;
        this.templateFactory = options.templateFactory;
        this.templateUrlFactory = options.templateUrlFactory;
    }

}

type DirectiveViewAnnotationConstructor = (options: DirectiveViewOptions) => ClassDecorator;
export var DirectiveView = <DirectiveViewAnnotationConstructor> makeDecorator(DirectiveViewAnnotation);