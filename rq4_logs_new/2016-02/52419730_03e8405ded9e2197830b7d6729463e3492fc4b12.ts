declare namespace __ResourceCollection {
    type ResourceAction = Function;

    interface ResourceModule<ResourceActionType> {
        name: string;
        pathPrefix?: string;
        index?: ResourceActionType;
        new?: ResourceActionType;
        new_?: ResourceActionType;
        create?: ResourceActionType;
        show?: ResourceActionType;
        edit?: ResourceActionType;
        update?: ResourceActionType;
        updateByPost?: ResourceActionType;
        destroy?: ResourceActionType;
        delete?: ResourceActionType;
    }

    declare class ActionMethod {
        value: string;
        constructor(value: string);
        static ALL: ActionMethod;
        static GET: ActionMethod;
        static POST: ActionMethod;
        static PUT: ActionMethod;
        static DELETE: ActionMethod;
        static PATCH: ActionMethod;
        static OPTIONS: ActionMethod;
        static HEAD: ActionMethod;
        static fromValue(value: string): ActionMethod;
    }

    interface ResourceEndpoint {
        /**
         * HTTP Method
         * @type {ActionMethod}
         */
        method: ActionMethod;
        /**
         * The HTTP path to the endpoint
         * @type {string}
         */
        path: string;
        /**
         * Name of the action
         * @type {string}
         */
        name: string;
    }

    interface Resource<ResourceActionType> {
        handler: ResourceModule<ResourceActionType>;
        endpoints: {
            [key: string]: ResourceEndpoint;
        };
    }

    interface SerializedResourceEndpoint {
        /**
         * HTTP Method
         * @type {string}
         */
        method: string;
        /**
         * The HTTP path to the endpoint
         * @type {string}
         */
        path: string;
        /**
         * Name of the action
         * @type {string}
         */
        name: string;
    }

    interface SerializedResource {
        handler: {
            [key: string]: string;
        };
        endpoints: {
            [key: string]: SerializedResourceEndpoint;
        };
    }

    declare class ResourceCollection<ResourceActionType> {
        globalPathPrefix: string;
        resources: {
            [key: string]: Resource<ResourceActionType>;
        };
        constructor(globalPathPrefix?: string);
        getEndpoint(resource: string, action: string): ResourceEndpoint;
        /**
         * Add a resource to the ResourceCollection instance.
         * @param  {string}             name           name of the resource.
         * @param  {Resource}           resourceModule the module with action
         *                                             handlers
         * @param  {ResourceEndpoint[]} extra          non-restful endpoints.
         * @return {void}
         */
        resource(name: string, resourceModule: ResourceModule<ResourceActionType>, extra?: ResourceEndpoint[]): void;
        registerResourceModule(name: string, resourceModule: ResourceModule<ResourceActionType>): void;
        hasAction(resourceName: string, actionName: string): boolean;
        toJson(): any;
        /**
         * Restore the endpoints in the serialied ResourceCollection json instance.
         * @param {SerializedResource|string}} json The serialized
         *                                          ResourceCollection instance from
         *                                          toJson method.
         */
        loadFromJson(json: any): void;
    }

    declare class PathHelpers<ResourceActionType> {
        resources: ResourceCollection<ResourceActionType>;
        urlRoot: string;
        constructor(resources: ResourceCollection<ResourceActionType>, urlRoot?: string);
        /**
         * Return the path to the specified resource action.
         * @param  {string}             resource  The name of the resource.
         * @param  {string}             action    The name of the action.
         * @param  {string|string[]}    args      The arguments to be supplied to
         *                                        the path params. Formatting is
         *                                        done by utils.fillString function.
         * @param  {dict}               getParams The url's get parameters.
         * @return {string}                       The final url.
         */
        pathTo(resource: string, action: string, args?: any | any[], getParams?: {
            [key: string]: any;
        }): string;
        genPathTo(resource: string, action: string): (args?: any, getParams?: {
            [key: string]: any;
        }) => string;
        urlTo(resource: string, action: string, args?: any | any[], getParams?: {
            [key: string]: any;
        }): string;
        genUrlTo(resource: string, action: string): (args?: any, getParams?: {
            [key: string]: any;
        }) => string;
    }
}

declare module 'resource-collection' {
    exports = _Resourceful
}