/// <reference path="../typings/tsd.d.ts" />
interface String {
    format(...replacements: string[]): string;
}
declare module WiM.Models {
    interface ICitation {
        Title: string;
        Author: string;
        imgSrc: string;
        src: string;
    }
}
declare module StreamStats.Models {
    interface IKeyValue {
        Key: any;
        Value: any;
    }
}
declare module WiM.Models {
    interface IParameter {
        name: string;
        code: string;
        unit: string;
        value: number;
    }
}
declare module WiM.Models {
    interface IPoint {
        Latitude: number;
        Longitude: number;
        crs: string;
    }
    class Point implements IPoint {
        Latitude: number;
        Longitude: number;
        crs: string;
        constructor(lat: number, long: number, crs: string);
        ToEsriString(): string;
        static FromJson(json: Object): Point;
    }
}
declare var configuration: any;
declare module WiM.Services {
    interface IAuthenticationBase {
        SetTokenAuthentication(uri: string, password: string): void;
        SetBasicAuthentication(uri: string, password: string): void;
    }
    interface IUser {
        username: string;
        role: string;
    }
    class AuthenticationServiceAgent extends HTTPServiceBase implements IAuthenticationBase {
        private User;
        constructor($http: ng.IHttpService, $q: ng.IQService, baseURL: string, u: IUser);
        SetBasicAuthentication(uri: string, password: string): ng.IPromise<void>;
        SetTokenAuthentication(uri: string, password: string): ng.IPromise<void>;
        private encode(input);
    }
}
declare module WiM.Services.Helpers {
    class RequestInfo implements ng.IRequestConfig {
        method: string;
        url: string;
        dataType: string;
        params: any;
        data: any;
        constructor(ul: string, mthd?: methodType, dtype?: string, data?: any);
    }
    enum methodType {
        GET = 0,
        POST = 1,
        PUT = 2,
        DELETE = 3,
    }
}
declare module WiM.Services {
    interface IHTTPServiceBase {
        Execute(request: Helpers.RequestInfo): ng.IPromise<any>;
    }
    class HTTPServiceBase implements IHTTPServiceBase {
        private baseURL;
        $http: ng.IHttpService;
        constructor(http: ng.IHttpService, baseURL: string);
        Execute<T>(request: Helpers.RequestInfo): ng.IPromise<T>;
    }
}
declare module WiM.Services {
    interface ISearchAPIOutput extends Models.IPoint {
        Name: string;
        Category: string;
        State: string;
        County: string;
        Elevation: number;
        Id: string;
        Source: string;
    }
    interface ISearchAPIService {
        getLocations(searchTerm: string): ng.IPromise<Array<ISearchAPIOutput>>;
    }
    class SearchLocation implements ISearchAPIOutput {
        Name: string;
        Category: string;
        State: string;
        County: string;
        Longitude: number;
        Latitude: number;
        crs: string;
        Elevation: number;
        Id: string;
        Source: string;
        constructor(nm: string, ct: string, st: string, lat: number, long: number);
    }
}
declare module WiM.Services {
}