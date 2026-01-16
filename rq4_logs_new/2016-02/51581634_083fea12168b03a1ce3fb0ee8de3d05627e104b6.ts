declare module Spa {
    class App {
        static namespace: Object;
        static container: string;
        init(): void;
        /**
         * Generates the html for rendering the set route template
         * @param  {object} data      to pass to template
         * @param  {string} template  name to render
         * @param  {string} container id which template should replace
         */
        static generateHtml(data: Object, template: string): void;
        /**
         * Renders the error page
         */
        static renderErrorPage(): void;
    }
}
declare module Spa {
    abstract class Route implements Spa.iRoute {
        private route;
        private title;
        container: string;
        constructor(route: string);
        setRouteTitle(title: string): void;
        getRouteTitle(): string;
        getRoute(): string;
        /**
         * Initializes routes
         */
        abstract initRoutes(): any;
        addRoute(path: string, func: Function): void;
        /**
         * Renders a hbs template file, defined in views folder:
         * views/templates/pages/*
         */
        render(template: string): void;
    }
}
declare module Spa {
    class RouteMapper {
        static init(): void;
        /**
         * Changes the current hash without notifying Crossroads, to prevent any parsing and redirecting of hash
         * @param  {string} hash to silently change
         */
        static setHashSilently(hash: string): void;
        static setHash(hash: string): void;
        static isHash(hash: string): boolean;
        /**
         * Add a route to which should be searched on a hashChange event
         * @param  {string} path to listen for
         * @param  {Function} func to run if route matches
         */
        static addRoute(path: string, func: Function): void;
        static hashChange(newHash: string, oldHash: string): void;
    }
}
declare module Spa {
    interface iRoute {
        container: string;
        setRouteTitle(title: string): any;
        getRouteTitle(): string;
        getRoute(): string;
        initRoutes(): any;
        addRoute(path: string, func: Function): any;
        render(template: string): any;
    }
}