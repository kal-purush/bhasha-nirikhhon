/// <reference path="../../typings/tsd.d.ts"/>
import {Redirect} from 'aurelia-router';

export class App {
    public router;

    configureRouter(config, router) {
        this.router = router;
        var rootPrefix = 'root';
        config.title = 'App';
        config.map([
            {route: rootPrefix, name: 'root', moduleId: 'app/root/root'},
            {route: [''], redirect: rootPrefix}
        ]);

        config.mapUnknownRoutes(instruction => {
            instruction.config.redirect = rootPrefix;
        });
    }
}