/// <reference path="../typings/tsd.d.ts" />

module Dashboard.Routes {
    
    export class AppRouter extends Backbone.Router {
    
        defaults() {
            return {
                routes: {
                    "for/:username": "forUsername"    
                }
            }
        }
    }
}