declare module "elasticsearch" {

    interface Callback {
        (err:Error, response:any, status:number): any;
    }

    interface Promise {
        then (success: (response) => void, failed: (error) => void): void;
    }

    interface getParams {
        index: string;
        type?:string;
        id:any;
    }

    interface Indices {
        open (params, callback:Callback):void;
        close (params, callback:Callback):void;
        create (params):Promise;
        delete (params, callback:Callback):void;
        exists (params):Promise;
        existsType (params, callback:Callback):void;
        putMapping (params):Promise;
        putSettings(params, callback:Callback):void;
    }

    interface Cluster {
        health (param, callback:Callback):void;
    }

    class Client extends ClientInterface {
        constructor(config: any);
    }

    class ClientInterface {
        create (params): Promise;

        delete (params:{
            index:string;
            type:string;
            id:any;
        }, callback:Callback);

        deleteByQuery(params:{
            index:string;
            type?:string;
            body: Object;
        }, callback:Callback);

        exists (params:{
            index:string;
            type:string;
            id:string;
        }, callback:(err:Error, exists:boolean) => any):void;

        get (params:getParams, callback:Callback):void;

        getSource (params:getParams, callback:Callback):void;

        indices:Indices;
        cluster:Cluster;

        index (params:{
            index:string;
            type:string;
            body:Object;
        }, callback:Callback):void;

        percolate (params:{
            index:string;
            body:Object;
        }, callback:Callback):void;

        ping (params, callback:Callback):void;

        search (params:{
            index: string;
            body: Object;
        }, callback:Callback):void;

    }

    interface ClientStaticInterface {
        (options?):ClientInterface;
    }

    //export var Client:ClientStaticInterface;
}