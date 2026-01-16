export declare abstract class BaseControllerMongoose {
    req: any;
    res: any;
    static router: any;
    get(res: any, model: any): void;
    cget(res: any, model: any): void;
    post(model: any, form: any, request: any, response: any): void;
    put(model: any, form: any, request: any, response: any): void;
    static processForm(model: any, form: any, request: any, response: any): void;
    delete(res: any, object: any): void;
}