/// <reference path="../lib/angularjs/angular.d.ts" />
/// <reference path="../lib/angularjs/angular-resource.d.ts" />
/// <reference path="../lib/angularjs/angular-animate.d.ts" />
/// <reference path="../lib/angularjs/angular-route.d.ts" />
/// <reference path="../lib/angularjs/angular-sanitize.d.ts" />

window["Controller"] = function () { };

/// base class for all controller
declare class Controller implements ng.IScope {
    public controllerName: string
    public injectModules: string[]
    prototype: any

    public onLoad()


    [index: string]: any

    $apply(): any
    $apply(exp: string): any
    $apply(exp: (scope: ng.IScope) => any): any

    $applyAsync(): any
    $applyAsync(exp: string): any
    $applyAsync(exp: (scope: ng.IScope) => any): any

    /**
        * Dispatches an event name downwards to all child scopes (and their children) notifying the registered $rootScope.Scope listeners.
        *
        * The event life cycle starts at the scope on which $broadcast was called. All listeners listening for name event on this scope get notified. Afterwards, the event propagates to all direct and indirect scopes of the current scope and calls all registered listeners along the way. The event cannot be canceled.
        *
        * Any exception emitted from the listeners will be passed onto the $exceptionHandler service.
        *
        * @param name Event name to broadcast.
        * @param args Optional one or more arguments which will be passed onto the event listeners.
        */
    $broadcast(name: string, ...args: any[]): ng.IAngularEvent
    $destroy(): void
    $digest(): void
    /**
        * Dispatches an event name upwards through the scope hierarchy notifying the registered $rootScope.Scope listeners.
        *
        * The event life cycle starts at the scope on which $emit was called. All listeners listening for name event on this scope get notified. Afterwards, the event traverses upwards toward the root scope and calls all registered listeners along the way. The event will stop propagating if one of the listeners cancels it.
        *
        * Any exception emitted from the listeners will be passed onto the $exceptionHandler service.
        *
        * @param name Event name to emit.
        * @param args Optional one or more arguments which will be passed onto the event listeners.
        */
    $emit(name: string, ...args: any[]): ng.IAngularEvent

    $eval(): any
    $eval(expression: string, locals?: Object): any
    $eval(expression: (scope: ng.IScope) => any, locals?: Object): any

    $evalAsync(): void
    $evalAsync(expression: string): void
    $evalAsync(expression: (scope: ng.IScope) => any): void

    // Defaults to false by the implementation checking strategy
    $new(isolate?: boolean, parent?: ng.IScope): ng.IScope

    /**
        * Listens on events of a given type. See $emit for discussion of event life cycle.
        *
        * The event listener function format is: function(event, args...).
        *
        * @param name Event name to listen on.
        * @param listener Function to call when the event is emitted.
        */
    $on(name: string, listener: (event: ng.IAngularEvent, ...args: any[]) => any): Function

    $watch(watchExpression: string, listener?: string, objectEquality?: boolean): Function
    $watch<T>(watchExpression: string, listener?: (newValue: T, oldValue: T, scope: ng.IScope) => any, objectEquality?: boolean): Function
    $watch(watchExpression: (scope: ng.IScope) => any, listener?: string, objectEquality?: boolean): Function
    $watch<T>(watchExpression: (scope: ng.IScope) => T, listener?: (newValue: T, oldValue: T, scope: ng.IScope) => any, objectEquality?: boolean): Function

    $watchCollection<T>(watchExpression: string, listener: (newValue: T, oldValue: T, scope: ng.IScope) => any): Function
    $watchCollection<T>(watchExpression: (scope: ng.IScope) => T, listener: (newValue: T, oldValue: T, scope: ng.IScope) => any): Function

    $watchGroup(watchExpressions: any[], listener: (newValue: any, oldValue: any, scope: ng.IScope) => any): Function
    $watchGroup(watchExpressions: { (scope: ng.IScope): any }[], listener: (newValue: any, oldValue: any, scope: ng.IScope) => any): Function

    $parent: ng.IScope
    $root: ng.IRootScopeService
    $id: number

    // Hidden members
    $$isolateBindings: any
    $$phase: any
}


class FactoryDefinition {

    public name: string

    public dependencies: string[]

    public method: Function

    /// initilize a Factory Definition object
    constructor(name: string, dependencies: string[], method: Function) {
        this.name = name
        this.dependencies = dependencies
        this.method = method
    }
}

/// base class for all application
class Module {

    /// the required modules
    requiredModules: string[]

    /// the application name
    moduleName: string

    /// the application module
    module: ng.IModule

    /// factory definitions
    factories: FactoryDefinition[]

    /// name of the constructor
    constructor(name?: string, dependencies?: string[]) {

        if (name !== undefined) {
            this.moduleName = name
        }

        if (dependencies !== undefined) {
            this.requiredModules = dependencies
        }
    }

    /// register the application
    public register() {

        if (this.requiredModules === undefined) {
            this.requiredModules = []
        }

        this.module = angular.module(this.moduleName, this.requiredModules)


        if (this.factories !== undefined) {

            /// register factory methods
            for (var i = 0; i < this.factories.length; i++) {
                var factory = this.factories[i]
                var definition: any[] = factory.dependencies
                definition.push(factory.method)

                this.module.factory(factory.name, definition)
            }
        }
    }

    /// register the controller
    public registerController(controller: Controller) {

        var description: any[] = ["$scope"].concat(controller.injectModules)

        description.push(function () {
            var $scope = arguments[0] as Controller
        
            // copy properties
            for (var property in controller) {
                $scope[property] = controller[property]
            }

            // copy injected modules
            for (var i = 0; i < controller.injectModules.length; i++) {
                var moduleName = controller.injectModules[i]
                $scope[moduleName] = arguments[i + 1]
            }

            $scope.onLoad()
        })

        
        this.module.controller(controller.controllerName, description)
    }
}


/// @controller
function controller(name: string) {
    /// the controller name
    return function (target: Function) {
        target.prototype.controllerName = name
    }
}

/// @application decorator
function module(name: string) {
    return function (target: Function) {
        target.prototype.moduleName = name
    }
}


/// @inject modules
function wired(target: any, propertyKey: string) {
    var controller = target as Controller;

    if (controller.injectModules === undefined) {
        controller.injectModules = []
    }

    controller.injectModules.push(propertyKey)
}

/// @required decorator
function required(modules: string[]) {

    return function (target: Function) {
        target.prototype.requiredModules = modules
    }
}

/// @factory in an application
function factory(name: any = undefined, dependencies: string[] = []) {

    /// add a factory definition to the application class
    return function (target: Module, propertyKey: string, descriptor: TypedPropertyDescriptor<any>) {

        if (Array.isArray(name) && dependencies === undefined) {
            dependencies = name
            name = undefined
        }

        if (name === undefined) {
            name = propertyKey.replace(/^get/, "")
            name = name.charAt(0).toLowerCase() + name.slice(1)
        }

        var definition = new FactoryDefinition(name, dependencies, descriptor.value)

        if (target.factories === undefined) {
            target.factories = []
        }

        target.factories.push(definition)
    }
}