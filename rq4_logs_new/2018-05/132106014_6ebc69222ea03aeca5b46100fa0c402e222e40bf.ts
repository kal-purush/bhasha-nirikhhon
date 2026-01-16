// the polyfills must be one of the first things imported in node.js.
// The only modules to be imported higher - node modules with es6-promise 3.x or other Promise polyfill dependency
// (rule of thumb: do it if you have zone.js exception that it has been overwritten)
// if you are including modules that modify Promise, such as NewRelic,, you must include them before polyfills
require('newrelic');
import 'zone.js/dist/zone-node';
import 'reflect-metadata';
import 'ts-helpers';

import * as path from 'path';
import * as express from 'express';
import * as bodyParser from 'body-parser';
import * as cookieParser from 'cookie-parser';
import * as compression from 'compression';
import { ngApp } from "../src/server/middleware/app";


import { enableProdMode } from '@angular/core';

import { ngExpressEngine } from '@nguniversal/express-engine';

// App
import { MainModule } from './node.module';


// ** Example API
import { api } from "./server/routes/api";
import { globalErrorHandler } from "./server/middleware/error";
import * as helmet from "helmet";
import { pages } from "./server/routes/pages";
let responseTime = require("response-time");


export class Server {

       private static _instance: Server;
       private app: express.Express;

       constructor() {
      
          // For Singleton class
          if (Server._instance) {
            throw new Error("Error: 'Server is already initialized.'");
          }
      
      
          // enable prod for faster renders
          enableProdMode();
          this.app = express();
          this.config();
          this.routers();
      
          this.app.listen(3000, () => {
            console.log("Listening on:: http://localhost:3000");
          });
        }


        public static bootstrap(): Server {
          Server._instance = new Server();
          return Server._instance;
        }




      // enableProdMode();
      // const app = express();
      // app.set('port', process.env.PORT || 3000);
      // let server = app.listen(app.get('port'), () => {
      //   console.log(`Listening on: http://localhost:${server.address().port}`);
      // });
      
      // config();
      // routers();
      
      private config(): void {
        this.app.use(responseTime());
        this.app.engine('.html', ngExpressEngine({
          bootstrap: MainModule,
          providers: [
            // use only if you have shared state between users
            // { provide: 'LRU', useFactory: () => new LRU(10) }
        
            // stateless providers only since it's shared
          ]
        }));
        //ToDo: remove hardcoding 
        this.app.set('views', 'dist');
        this.app.set('view engine', 'html');
        this.app.set('json spaces', 2);
      this.app.use(cookieParser('Angular 2 Universal'));
        this.app.use(bodyParser.json());
        this.app.use(bodyParser.urlencoded({
          extended: false,
        }));
        this.app.use(compression());
        this.app.use(helmet());

      
      

        // Serve static files
        //app.use('/assets', cacheControl, express.static(path.join(__dirname, 'assets'), {maxAge: 30}));
        this.app.use(function cacheControl(req, res, next) {
          // instruct browser to revalidate in 60 seconds
          // res.header('Cache-Control', 'max-age=60');
          next();
        }, express.static(path.join(__dirname, 'dist/client'), {index: false}));
        
        
         this.app.use("/client", express.static("dist/client", {
            index: false,
            setHeaders: function (res: express.Response, path: string, stat: any) {
              // res.set("Content-Encoding", "gzip");
            res.set("Cache-Control", "public, max-age=31557600");
              // res.set("Content-Type","text/javascript");
            },
          }));

 
      }

      
      private initApi(): void {
         this.app.use("/mobile/api", api);
      }

      private initPages() {
        pages(this.app);
      }
      
      private routers(): void {
        // Our API for demos only
        this.initApi();
        this.initPages();
        this.app.use(globalErrorHandler);
      
      }
}


Server.bootstrap();