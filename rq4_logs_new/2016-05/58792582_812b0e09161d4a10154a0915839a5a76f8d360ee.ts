
/**
 * Created by igor on 1/13/16.
 */
interface URLSearchParams {
    append: (name:string, value:string) => void;
    delete: (name:string) => void;
    entries: () => {
        key: string;
        value: string;
    }[];
    get: (name:string) => string;
    getAll: (name:string) => string[];
    keys: (name:string) => string[];
    set: (name:string, value:string) => void;
    values: any[];
}
declare var URLSearchParams:{
    prototype: URLSearchParams;
    new (): URLSearchParams;
};
interface Headers {
    append: (name:string, value:string) => void;
    delete: (name:string) => void;
    entries: () => {
        key: string;
        value: string;
    }[];
    get: (name:string) => string;
    getAll: (name:string) => string[];
    keys: (name:string) => string[];
    set: (name:string, value:string) => void;
}
declare var Headers:{
    prototype: Headers;
    new (): Headers;
};
interface InitRequest {
    method: string;
    headers: string | typeof Headers;
    mode: string;
    credentials: string;
    cache: string;
    redirect: string;
    referrer: string;
}
interface Request {
    method: string;
    url: string;
    context: string;
    headers: typeof Headers;
    referrer: string;
    mode: string;
    credentials: string;
    cashe: string;
    bodyUsed: boolean;
    integrity: any;
    redirect: any;
    clone: () => Request;
}
declare var Request:{
    prototype: Request;
    new (input:string | Request, init?:InitRequest): Request;
};
interface InitResponse {
    status:string;
    statusText:string;
    headers:string|Headers;
}
interface ResponseBody {
    arrayBuffer:()=> PromiseLike<ArrayBuffer>;
    blob:()=> PromiseLike<Blob>;
    json():PromiseLike<any>;
}
interface Response extends ResponseBody {}

declare var Response:{
    prototype: Response;
    new (input:ResponseBody, init:InitResponse): Response;
};
declare function fetch(input:string | Request):PromiseLike<Response>;
declare type opt = {
    elem: HTMLElement;
    uri: string;
    queryMethod: string;
    apiKey: string;
};
interface IPhoto {
    farm: number;
    id: string;
    isfamily: number;
    isfriend: number;
    ispublic: number;
    owner: string;
    secret: string;
    server: string;
    title: string;
}
declare var Q:{
    all:(arr:PromiseLike<any>[]) => PromiseLike<any>
};