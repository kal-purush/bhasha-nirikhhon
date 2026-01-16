/// <reference path="./_references" />

import {bind} from './di';
import {makeDecorator, create, merge, Map, forEach} from './utils';
import {getAnnotations} from './reflection';

/**
 * @internal
 */
export class RoutesAnnotation {
    
    routes: Map<string|Function>;

    constructor(routes: Map<string|Function>) {
        this.routes = routes;
    }

}

type RoutesDecorator = (routes: Map<string|Function>) => ClassDecorator;

/**
 * A decorator to annotate a class with states
 */
export var Routes = <RoutesDecorator> makeDecorator(RoutesAnnotation);

/**
 * @internal
 */
export function registerRoutes(moduleController: Function, ngModule: ng.IModule) {
    
    var notes = <RoutesAnnotation[]> getAnnotations(moduleController, RoutesAnnotation);
        
    if (!notes.length) return;
    
    var routes = {};
    notes.forEach((note) => merge(routes, note.routes));
    
    ngModule.config(bind(['$urlRouterProvider'], ($urlRouterProvider: ng.ui.IUrlRouterProvider) => {
        
        forEach(routes, (handler, route) => {
            if (route === '?') {
                $urlRouterProvider.otherwise(handler);
            }
            else {
                $urlRouterProvider.when(route, handler);
            }
        });
                
    })); 
    
}