module jDebug {
    /**
     * Injector overrides default angular injector and allows to drop directive factory cache
     * for dynamic updating directive definition
     */
    export class JDebugDirectiveInjector {
        private originalGet:Function;
        private cache = {};
        private factories = {};

        constructor(private injector:any) {
            this.originalGet = injector.get;
            injector.get = this.get.bind(this);
        }

        get(serviceName:string, caller?:any) {
            if (this.cache.hasOwnProperty(serviceName)) {
                //override
                return this.cache[serviceName];
            } else {
                if (this.factories.hasOwnProperty(serviceName)) {
                    //override
                    return this.cache[serviceName] = this.factories[serviceName]();
                } else {
                    return this.originalGet(serviceName, caller);
                }
            }
        }

        updateDirective(directiveName:string, factory:Function) {
            var factoryName = directiveName + 'Directive';

            this.factories[factoryName] = factory;
            delete this.cache[factoryName];
        }
    }
}