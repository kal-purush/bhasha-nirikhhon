require('zone.js');

import * as express from 'express';


type ExpressCallback = (req: express.Request, res: express.Response) => void;

export class RouterUtil {

    static zone = Zone.current.fork({
        name: 'snow_monky',
    });

    static counter = 0;

    static _zone(fn: Function): Function {
        return function(req: express.Request, res: express.Response) {
            let counter = RouterUtil.counter++;
            RouterUtil.zone.fork({
                name: 'R' + counter
            }).run(() => {
                fn.apply(this, [req, res]);
            });
        }
    }

    static wrap(fn: Function, source: string): Function {
        let counter = RouterUtil.counter++;
        return function() {
            if (typeof arguments[1] === 'function') {
                arguments[1] = RouterUtil._zone(arguments[1]);
            }
            fn.apply(this, arguments);
        };
    }

    static inZone(input: express.Application): express.Application {
        let app: any = <any>input;

        ['get', 'put', 'post', 'delete'].forEach((method) => {
            let originalMethod = app[method];
            app[method] = RouterUtil.wrap(app[method], method);
            
            //  (path: string, callback: (req: express.Request, res: express.Response) => void) => {
            //     originalMethod.apply(app, [path, (req: express.Request, res: express.Response) => {
            //         // let c = RouterUtil.counter++
            //         // let zone = RouterUtil.zone.fork(
            //         //     {
            //         //         name: 'z' + c,

            //         //         onHandleError: (delegate, current, target, error) => {
            //         //             console.error(target.name, error.rejection.stack);
            //         //             res.status(500).json({
            //         //                 message: 'Unexpected Error'
            //         //             });
            //         //             return false;
            //         //         }

            //         //     });

            //         RouterUtil.zone.runGuarded(() => {
            //             callback(req, res);
            //         });

            //     }]);
            // };
        });
        return app;
    }
}