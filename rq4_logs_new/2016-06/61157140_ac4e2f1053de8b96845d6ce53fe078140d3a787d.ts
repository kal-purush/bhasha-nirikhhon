import { Server, IServerOptions, IServerConnectionOptions } from 'hapi';

interface Options {
    relativeTo?: string;
    preConnections?: (Server:Server, next:(err:any)=>void ) => void;
    preRegister?: (Server:Server, next:(err:any)=>void ) => void;
}

interface Plugin {
    plugin: string | {
        register:string;
        options?:any;
    };
    options?: any;
}

interface Manifest {
    server: IServerOptions;
    connections: Array<IServerConnectionOptions>;
    registrations?: Array<Plugin>;
}

declare namespace glue {
    export function compose(manifest: Manifest, options?: Options, callback?: (err?: any, server?: Server) => void);
}

export = glue;
