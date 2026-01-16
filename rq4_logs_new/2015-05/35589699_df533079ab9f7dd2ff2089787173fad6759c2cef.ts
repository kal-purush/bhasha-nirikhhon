/*
Resultdo esperado:

Uso em construtor da classe:
	class MyClass {
		constructor(@Inject('injectable1') servico1:any, @Inject('injectable2') servico2:any) {
		}
	}

MyClass.$inject = ['injectable1', 'injectable2'];

Uso em metodo:
	class MyClass {
		metodo(@Inject('injectable1') servico1:any, @Inject('injectable2') servico2:any) {
		}
	}

MyClass.metodo.$inject = ['injectable1', 'injectable2'];

Uso em função:
	function funcao(@Inject('injectable1') servico1:any, @Inject('injectable2') servico2:any) {
	}

funcao.$inject = ['injectable1', 'injectable2'];

*/

export function Inject<T extends Function>(injectablesNames:string[], target:T):T;

export function Inject(injectableName:string):ParameterDecorator;

export function Inject(a:any, b?:any):any {
	
	switch (arguments.length) {
		case 1:
            // Used as decorator
			return decoratorImplementation(a);			
		case 2:
            // Used directly on a function
			return functionImplementation(a, b);
	}
    
    // TODO throw exception for misuse?
	
}

function functionImplementation<T extends Function>(injectablesNames:string[], func:T):T {
	
    func.$inject = injectablesNames.slice();
	return func;
	
}

function decoratorImplementation(injectableName:string):ParameterDecorator {
	
	return (target:Function, propertyKey:string|symbol, parameterIndex:number) => {
        
        // If propertyKey is defined, we're decorating a parameter of a method
        // If not, we're decorating a parameter of class constructor
		target = (typeof propertyKey == 'undefined') ? target : target = (<any>target)[propertyKey];
        
		var $inject:string[] = (target.$inject = target.$inject || []);
		$inject[parameterIndex] = injectableName;
				 
	}
	
}