import {ExpressSpringApplicationImpl} from "./Interfaces";
import {Logger} from "logger20js-ts";
import {AbstractProperties, Properties} from "lib-utils-ts/src/file/Properties";
import {MiddleWare} from "./MiddleWare";
import {Spring} from "./Spring";
import {IPropertiesFile, MapType} from "lib-utils-ts/src/Interface";
import * as express from "express";
import * as https from "https";
import {Application} from "./Application";

export abstract class ExpressSpringApp implements ExpressSpringApplicationImpl{

    private readonly applicationWrap: Application = Application.getInstance();
    private static readonly logger:Logger  = Logger.factory(Application.name);

    private prop:AbstractProperties<Object> = Application.getInstance().getProperties();

    protected baseUrl:string = "/v1";
    protected middleWare: MiddleWare;
    protected mockUserAccess:Spring.AUTH_LEVEL = -1;

    protected constructor( properties: IPropertiesFile<string, Object> = null ) {

        if(properties){
            this.prop = <Properties>properties;
            this.baseUrl = <string>properties.getProperty("baseUrl", null );
        }else if(!this.prop){
            // minimal Configuration
            this.prop = new Properties();
            this.prop.setProperty("httpProtocol", true);
            this.prop.setProperty("httpProtocol", false);
            this.prop.setProperty("gateway", "gateway");
            this.prop.setProperty("loggerParser ", 80);
            this.prop.setProperty("loggerParser", "[%hours{yellow}] %T{w?yellow;e?red;d?green}/%name - %error");
            this.prop.setProperty("logEnabledColorize", true);
            this.prop.setProperty("saveLog", false);
        }
        this.middleWare = new MiddleWare(this);
    }

    public setBaseUrl( baseUrl:string ): ExpressSpringApp{
        this.baseUrl = baseUrl;
        return this;
    }

    public getBaseUrl( ):string{return String(this.prop.getProperty("gateway", this.baseUrl));}

    public setMockDefaultUserAccess( level:Spring.AUTH_LEVEL ):ExpressSpringApplicationImpl {
        this.mockUserAccess = level;
        return this;
    }

    public getMockDefaultUserAccess( ):Spring.AUTH_LEVEL {
        return this.mockUserAccess;
    }

    public getApp( ):express.Application{return this.applicationWrap.getApp();}

    public getMiddleWare(): MiddleWare { return this.middleWare; }
    /***
     * STEP 0
     */
    public loadProperties( path:string ):ExpressSpringApplicationImpl {
        this.applicationWrap.loadConfiguration(path);
        this.prop = this.applicationWrap.getProperties();
        return this;
    }
    /***
     * STEP 1
     * Initialize all middleware before initialize initPages methods
     * see MiddleWare Class
     */
    public config(): ExpressSpringApplicationImpl { throw new Error("Not implemented !"); }
    /***
     * STEP 2
     */
    public initPages():ExpressSpringApplicationImpl{
        this.applicationWrap.init(String(this.prop.getProperty("pagesDirectory")));
        return this;
    }

    private getSSlOpts( ):MapType<string, string>{
        let sslOpts: MapType<string,string> = {};

        if(this.prop.getProperty("sslPfx", null)){
            sslOpts.pfx = String(this.prop.getProperty("sslPfx"));
            sslOpts.passphrase = String(this.prop.getProperty("sslPassphrase"));
        }else{
            sslOpts.cert = String(this.prop.getProperty("sslCert"));
            sslOpts.key = String(this.prop.getProperty("sslCertKey"));
        }
        if(this.prop.getProperty("sslCacert")) sslOpts.cacert = String(this.prop.getProperty("sslCacert"));
        return sslOpts;
    }

    public sslProtocol( ):void{
        if(Boolean.of(this.prop.getProperty("httpsProtocol"))) {
            let port:number = Number(this.prop.getProperty("sslPort", 80)),
                gateway:string = String(this.prop.getProperty("gateway", "0.0.0.0")),
                sslOpts: MapType<string,string> = {};

            https.createServer( this.getSSlOpts(), this.getApp()).listen(
                port, gateway,
                1,
                () => {
                    ExpressSpringApp.logger.info("SSL/TLS server has been started : %s:%s",gateway,port);
                });
        }
    }
    /****
     *  STEP 3
     * EndPoint for launch APP http or https
     */
    public listen( ):void {
        if(Boolean.of(this.prop.getProperty("httpProtocol"))) {
            let port:number = Number(this.prop.getProperty("port", 80)),
                gateway:string = String(this.prop.getProperty("gateway", "0.0.0.0"));
            this.getApp().listen(
                port, gateway,
                () => {
                    ExpressSpringApp.logger.info("server has been started : %s:%s",gateway,port);
                });
        }
        return this.sslProtocol();
    }
}