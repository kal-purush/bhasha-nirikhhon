import { Express, Request, Response, NextFunction } from 'express';

type Plugin = {
    handlers: {
        [path: string]: (req: Request, res: Response, next: NextFunction) => any
    }
};
const plugins: Array<Plugin> = [];

export function registerPlugin(plugin: Plugin) {
    if (plugins)
        plugins.push(plugin);
    else
        throw new Error('Server yet started');
}

export function initServer(app: Express) {
    plugins.forEach(p => {
        initHandlers(p); 
    })
    function initHandlers(plugin: Plugin) {
        Object.keys(plugin.handlers).forEach(path => {
            app.use(path, plugin.handlers[path]);
        })
    }
}