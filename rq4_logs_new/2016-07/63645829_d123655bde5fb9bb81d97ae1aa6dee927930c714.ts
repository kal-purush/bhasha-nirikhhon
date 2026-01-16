// IMPORTS
// =================================================================================================
import * as SocketIO from 'socket.io';
import { 
    Action, ActionAdapter, Executor, ExecutorContext, ExecutionOptions, AuthInputs, RateOptions,
    DaoOptions, Exception, validate
} from 'nova-base';

// INTERFACES
// =================================================================================================
export interface HandlerConfig<V,T> {
    defaults?       : any;
    adapter?        : ActionAdapter<V>;
    action          : Action<V,T>;
    rate?           : RateOptions;
    dao?            : DaoOptions;
    auth?           : any;
}

export interface SocketEventHandler {
    (data: any, callback: (response) => void)
}

// CLASS DEFINITION
// =================================================================================================
export class Listener {

    name        : string;
    root        : string;
    handlers    : Map<string, HandlerConfig<any,any>>;
    context     : ExecutorContext;

    // CONSTRUCTOR
    // --------------------------------------------------------------------------------------------
    constructor(name?: string) {
        this.name = name;
        this.handlers = new Map<string, HandlerConfig<any,any>>();
    }

    // PUBLIC METHODS
    // --------------------------------------------------------------------------------------------
    on<V,T>(event: string, config: HandlerConfig<V,T>) {

    }

    bind(server: SocketIO.Server, context: ExecutorContext) {

        this.context = context;

        server.on('connection', (socket) => {
            
            socket.on('authenticate', this.buildAuthHandler(socket));

            for (let [event, config] of this.handlers) {
                socket.on(event, this.buildEventHandler(config, socket));
            }
        });
    }

    // PRIVATE METHODS
    // --------------------------------------------------------------------------------------------
    private buildAuthHandler(socket: SocketIO.Socket): SocketEventHandler {
        if (!socket) return;
    }

    private buildEventHandler(config: HandlerConfig<any,any>, socket: SocketIO.Socket): SocketEventHandler {
        if (!config || !socket) return;

        // build execution options
        const options: ExecutionOptions = {
            daoOptions  : config.dao,
            rateOptions : config.rate,  // TODO: get default options from somewhere?
            authOptions : config.auth
        };

        // build executor
        const executor = new Executor(this.context, config.action, config.adapter, options);

        // build and return the handler
        return function(data: any, callback: (response) => void) {
            const inputs = Object.assign({}, config.defaults, data); 
            const authInputs: AuthInputs = undefined; // TODO: get auth data from socket
            executor.execute(inputs, authInputs)
                .then((result) => callback(undefined))
                .catch((error) => {
                    // TODO: log the error
                    callback(error);
                });
        }
    }
}