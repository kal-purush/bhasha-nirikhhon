/*
*/

import {makeDecorator} from '../utils';
import {ModuleOptions, ModuleAnnotation} from './module';

interface ApplicationOptions extends ModuleAnnotation {
	selector?:string;
}

class ApplicationAnnotation extends ModuleAnnotation {
	
	selector:string;
	
	constructor(options:ApplicationOptions) {
		super(options);
		this.selector = options.selector;
	}
	
}
	
type ApplicationAnnotationConstructor = (options:ApplicationOptions) => ClassDecorator;
var Application = <ApplicationAnnotationConstructor> makeDecorator(ApplicationAnnotation);

export {
    ApplicationOptions,
    ApplicationAnnotation,
    Application
};