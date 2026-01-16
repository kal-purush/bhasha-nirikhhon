/// <reference path="./_references" />

import {makeDecorator, setIfInterface} from './utils';
import {ViewOptions, ViewAnnotation} from './view';

/**
 * TODO document
 */
export const enum ComponentTemplateNamespace {
    HTML,
    SVG,
    MathML
}

/**
 * @internal
 */
export const NAMESPACE_MAP = ['html', 'svg', 'math'];

/**
 * Options available when decorating a component with view information
 * TODO document
 */
export interface ComponentViewOptions extends ViewOptions {
    
    /**
     * 
     */
    namespace?: ComponentTemplateNamespace;
    
    /**
     * @deprecated
     */
    replace?: boolean;
}

/**
 * @internal
 */
export class ComponentViewAnnotation extends ViewAnnotation {

    namespace = ComponentTemplateNamespace.HTML;
    replace = false;

    constructor(options: ComponentViewOptions) {
        super(options);
        setIfInterface(this, options);
    }

}

type ComponentViewDecorator = (options: ComponentViewOptions) => ClassDecorator;

/**
 * A decorator to annotate a component with view information
 */
export var ComponentView = <ComponentViewDecorator> makeDecorator(ComponentViewAnnotation);