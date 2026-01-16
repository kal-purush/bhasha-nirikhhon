import { RouteDefinition } from '@type';

export const Route = (requestMethod: 'get' | 'post', path: string): MethodDecorator => {
    return (target: object, propertyKey: string): void => {
        /*
         * Add this route (method) to the controller (class)
         */
        const controllerRoutes = Reflect.getMetadata('routes', target.constructor) as RouteDefinition[] || [];
        controllerRoutes.push({
            requestMethod,
            path,
            methodName: propertyKey,
        });
        Reflect.defineMetadata('routes', controllerRoutes, target.constructor);
    };
};