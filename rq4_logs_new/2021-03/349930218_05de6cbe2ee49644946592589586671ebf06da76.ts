import * as express from "express";
import {readdirSync} from "fs";
import {ArrayList} from "lib-utils-ts/src/List";
import {Predication} from "lib-utils-ts/src/Predication";
import * as bodyParser from "body-parser";
import * as https from "https";
import {IPropertiesFile} from "lib-utils-ts/src/Interface";
import {ExpressSpringApplicationImpl, SpringApplication} from "./Interfaces";
import { Logger} from "logger20js-ts";
import {MiddleWare} from "./MiddleWare";
import "lib-utils-ts/src/globalUtils";
/***
 *
*/
export class Application implements SpringApplication{

    private static readonly INSTANCE: Application = new Application();

    private logger:Logger  = Logger.factory(Application.class().name);
    private loaded:boolean = false;
    private readonly app: express.Application;

    constructor() {this.app = express();}

    public getApp( ):express.Application{return this.app;}

    public setLogger(logger:any): Application{
        this.logger = logger;
        return this;
    }

    public init( pages_directory:string ): SpringApplication{
        let p0: Predication<string> = new Predication<string>();
        let p1: Predication<string> = new Predication<string>();

        if(this.loaded) return this;
        p1.test = (value)=>value.endsWith(".d.ts");
        p0.test = (value)=>value.endsWith(".ts");

       // Logger.setLogStdout(false);
        ArrayList.of(readdirSync(pages_directory))
            .stream()
            .filter(p0.and(p1.negate()))
            .map(value=> value.explodeAsList(".").get(0) )
            .each(value=>{
                import(`${pages_directory}/${value}`);
                this.logger.debug(`import : ${pages_directory}/${value} page`)
            });
       // Logger.setLogStdout(true);
        this.loaded=true;
        return this;
    }

    public static getInstance():Application{ return Application.INSTANCE; }
}
/****
 *
*/
export abstract class ExpressSpringApp implements ExpressSpringApplicationImpl{

    private readonly applicationWrap: Application = Application.getInstance();

    protected prop: IPropertiesFile<string, Object>;
    protected baseUrl:string = "/v1";
    protected middleWare: MiddleWare;

    constructor( properties: IPropertiesFile<string, Object> ) {
        this.prop = properties;
        if(properties)this.baseUrl = <string>properties.getProperty("baseUrl", null );
        this.applicationWrap.init(<string>properties.getProperty("pagesDirectory"));
        this.middleWare = new MiddleWare(this);
    }

    public setBaseUrl( baseUrl:string ): ExpressSpringApp{
        this.baseUrl = baseUrl;
        return this;
    }

    public getBaseUrl( ):string{return this.baseUrl;}

    public getApp( ):express.Application{return this.applicationWrap.getApp();}

    public getMiddleWare(): MiddleWare { return this.middleWare; }

    public config(): ExpressSpringApplicationImpl { throw new Error("Not implemented !"); }

    public sslProtocol( ):void{
        if(<boolean>this.prop.getProperty("httpsProtocol")) {

            https.createServer({
                pfx: "",
                passphrase: ""
            }, this.getApp()).listen(
                <number>this.prop.getProperty("sslPort", 443),
                <string>this.prop.getProperty("gateway", "0.0.0.0"),
                1,
                () => {

                });
        }
    }

    public listen( ):void {
      if(<boolean>this.prop.getProperty("httpProtocol")) {
            this.getApp().listen(
                <number>this.prop.getProperty("port", 80),
                <string>this.prop.getProperty("gateway", "0.0.0.0"),
                () => {
                       // this.log
                });
        }
       return this.sslProtocol();
    }

}