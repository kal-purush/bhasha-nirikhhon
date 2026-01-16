declare var arep: AutoRouteExpressPromise.AutoRoute

declare module AutoRouteExpressPromise {

    type ControllerMethod = [AutoRouteBase.Method, (req: any) => Q.Promise<any>]

    interface RouteDefinition {

        /**
        * Route name, e.g., route: "/api/myroute/:id"
        */
        route: string

        /**
        * E.g., methods: [
        *   [method.get, req => myFunctionThatReturnsAPromiseGet(req.params["theIdName"])],
        *   [method.delete, req => myFunctionThatReturnsAPromiseDelete(req.params["theIdName"])]
        * ]
        */
        methods: ControllerMethod[]

    }

    interface Options {
        message?: (options: {routeName: string; methodName: string}) => void
        baseRoute: (routeName: string) => any
        sendWrapper?: (result: any) => any
    }

    interface ToControllersOptions extends Options {
        routeName: string
    }

    interface CreateRoutes {
        (routeDefinitions: RouteDefinition[]): void
    }

    interface AutoRoute {
        routes: (options: Options, glob: string[]) => void
        method: typeof AutoRouteBase.Method
    }
}

declare module "autoroute-express-promise" {
    export = arep
}