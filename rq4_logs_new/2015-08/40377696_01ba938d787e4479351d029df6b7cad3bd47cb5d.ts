/// <reference path="typings/typings.d.ts" />

import r = require('ramda')

interface RouteDefinition<T, S> {

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
    methods: [[T, S]]

}

export const subRoutes = <T, S>(routes: [string, any]): RouteDefinition<T, S>[] => {
    const makeRoute = key =>
        key[0] === '/'
            ? key
        : key !== '_methods'
            ? routes[0] + '/' + key
        : routes[0]

    return <any>r.is(Array, routes[1])
        ? <any>{ route: routes[0],
            methods: routes[1] }
    : r.pipe(
        r.toPairs,
        r.map(pair => [makeRoute(pair[0]), pair[1]]),
        r.map(subRoutes)
    )(routes[1])
}

export const flattenAltRoute =
    r.pipe(
        r.toPairs,
        r.reduce((acc, pairs: [string, any]) => acc.concat(subRoutes(pairs)), [])
    )


 
export const flattenRoute = <T, S>(routeDefinition: any): RouteDefinition<T, S>[] =>
    r.has('route', routeDefinition) && r.has('methods', routeDefinition)
        ? [routeDefinition]
    : flattenAltRoute(routeDefinition)
