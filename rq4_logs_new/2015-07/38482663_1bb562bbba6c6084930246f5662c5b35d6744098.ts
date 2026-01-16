/**
 * Created by daniil on 02.07.15.
 */
class Request {

    public onreadystatechange:()=>void = undefined;
    public readyState:number = 0;
    public timeout:number = 30000;
    public ontimeout:()=>void;
    public responseText:string = "";
    public status:number;
    public statusText:string;

    private type:string;
    private url:string;
    private async:boolean;
    private body:string;
    private headers = {};

    public open(type:string, url:string, async:boolean):Request {

        this.checkParams([type, url, async]);

        this.readyState = 1;
        this.type = type;
        this.url = url;
        this.async = async;

        return this;
    }

    public setRequestHeader(headerName:string, headerValue:string):Request {
        if (Request.hasNotValue(this.headers[headerName])) {
            this.headers[headerName] = headerValue;
        }
        return this;
    }

    public getResponseHeader(name:string):string {
        this.checkParams([name]);
        if (name != 'Set-Cookie' || name != 'Set-Cookie2') {
            return this.headers[name];
        }
    }

    public send(body?:string):Request {
        this.checkParams([this.type, this.url, this.async]);
        if (this.type == 'GET') {
            body = "";
        }
        this.startRequest();
        return this;
    }

    public abort():Request {

        return this;
    }

    private checkParams(params:Array<any>):void {
        if (!params.every((param:any) => !Request.hasNotValue(param))) {
            throw new Error('Неверные  параметры!');
        }
    }

    private startRequest():void {
        if (this.isAsync()) {
            Request.wait(Request.getInt(0, 50), () => {
                this.checkRequest();
                this.setNextStep();
            });
        } else {
            this.checkRequest();
        }
    }

    private setNextStep(repeatCount?:number):void {

        if (!repeatCount) {
            this.readyState++;
        }

        if (this.readyState == 3) {
            repeatCount = repeatCount ? repeatCount - 1 : Request.getInt(2, 4);
        } else {
            repeatCount = 0;
        }

        if (this.onreadystatechange) {
            this.onreadystatechange();
        }

        if (this.readyState != 4) {
            Request.wait(Request.getInt(0, 50), () => {
                this.setNextStep(repeatCount);
            });
        }
    }

    private static getInt(min, max):number {
        return Math.floor(Math.random() * (max - min + 1)) + min;
    }

    private checkRequest():void {

        var options = Request.urls[this.url];

        if (!options) {
            this.set404();
            return null;
        }

        if (options.types) {
            if (options.types.indexOf(this.type) == -1) {
                this.set404();
                return null;
            }
        }

        this.status = 200;
        this.responseText = options.response || '';
    }

    private set404() {
        this.status = 404;
        this.statusText = 'Not Found';
    }

    private isAsync():boolean {
        return this.async;
    }

    private static wait(time:number, callback:() => void):void {
        setTimeout(callback, time);
    }

    public static config(options):void {
        this.urls = options;
    }

    private static urls:Options = {};

    private static hasNotValue(some:any) {
        return some === undefined;
    }

}

export = Request;

interface Options {
    [url:string]: {
        types:Array<string>;
        response: string;
        time: number;
    }
}