/// <reference path="./_references" />

import {makeDecorator, FunctionReturningString, setIfInterface} from './utils';

/**
 * TODO document
 */
export const enum TemplateNamespace {
    HTML,
    SVG,
    MathML
}

/**
 * @internal
 */
export const NAMESPACE_MAP = ['html', 'svg', 'math'];

/**
 * Options available when decorating a class with template information
 * TODO document
 */
export interface TemplateOptions {
    
    /**
     * 
     */
    controllerAs: string;
    
    /**
     * 
     */
    inline?: string|FunctionReturningString;
    
    /**
     * 
     */
    url?: string|FunctionReturningString;
    
    /**
     * 
     */
    styleUrl?: string;
    
    /**
     * 
     */
    namespace?: TemplateNamespace;
    
    /**
     * @deprecated
     */
    replace?: boolean;
}

/**
 * @internal
 */
export class TemplateAnnotation {

    inline: string|FunctionReturningString = '';
    url: string|FunctionReturningString = '';
    styleUrl = '';
    controllerAs = '';
    namespace = TemplateNamespace.HTML;
    replace = false;

    constructor(options: TemplateOptions) {
        setIfInterface(this, options);
    }

}

type TemplateDecorator = (options: TemplateOptions) => ClassDecorator;

/**
 * A decorator to annotate a class with view information
 */
export var Template = <TemplateDecorator> makeDecorator(TemplateAnnotation);