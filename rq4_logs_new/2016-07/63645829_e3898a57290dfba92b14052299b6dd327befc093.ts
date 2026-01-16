// IMPORTS
// ================================================================================================
import { Application, AppOptions } from './lib/Application'

// MODULE VARIABLES
// =================================================================================================
export const defaults = {
    CORS: {
        origin      : '*',
        headers     : ['authorization', 'content-type', 'accept', 'x-requested-with', 'cache-control'],
        credentials : 'true',
        maxAge      : '1000000000'
    },
    rateLimits: {
        // TODO: find better names?
        anonymous   : undefined,
        identified  : undefined
    }
};

// PUBLIC FUNCTIONS
// ================================================================================================
export function createApp(options: AppOptions): Application {
    const app = new Application(options);
    return app;
}

// RE-EXPORTS
// =================================================================================================
export { Router } from './lib/Router';