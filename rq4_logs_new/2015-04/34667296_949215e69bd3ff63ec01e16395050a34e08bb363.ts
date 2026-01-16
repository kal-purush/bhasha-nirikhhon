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