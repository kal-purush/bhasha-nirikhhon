declare namespace Chai {
    interface Route {
        (path: string | RegExp, method: string): Assertion
    }

    interface Assertion {
        route: Route;
    }
}