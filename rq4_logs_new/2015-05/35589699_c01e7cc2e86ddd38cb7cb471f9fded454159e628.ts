/// <reference path="../_references" />

/**
 * Annotates a function with information of dependencies to be injected as parameters
 * 
 * * Overrides previous annotation (logs warning)
 * * All parameters should be annotated (logs warning)
 * * Dependencies will be injected in the order they are specified in the dependencies parameter
 * 
 * @param dependencies Names of the dependencies to be injected, in order
 * @returns The provided function
 */
export function bind<T extends Function>(dependencies: string[], func: T): T {
	
	// TODO warn about overriding annotation
	// TODO warn about mismatching number of parameters and dependencies

    func.$inject = dependencies.slice();
	return func;

}

/**
 * A decorator to annotate method parameterss with dependencies to be injected
 *
 * * Overrides previous annotation (logs warning)
 * * All parameters should be annotated (logs warning)
 * 
 * @param dependency The name of the dependency to be injected
 */
export function Inject(dependency: string): ParameterDecorator {

	return (target: Function, propertyKey: string|symbol, parameterIndex: number) => {
		
		// TODO warn about overriding annotation
		// TODO warn about mismatching number of parameters and dependencies
        
        // If propertyKey is defined, we're decorating a parameter of a method
        // If not, we're decorating a parameter of class constructor
		target = (typeof propertyKey == 'undefined') ? target : target = (<any>target)[propertyKey];
		
		// TODO what about missing elements in the $inject arrey?
		// ie. annotated the 2nd but not the 1st parameter

		var $inject: string[] = (target.$inject = target.$inject || []);
		$inject[parameterIndex] = dependency;

	}

}

/**
 * Inspects an object for dependency injection annotation
 * 
 * @internal
 * 
 * @param target The object to be inspected
 */
export function hasInjectAnnotation(target: any): boolean {
    
	return !target ? false : target.hasOwnProperty('$inject');
	
}