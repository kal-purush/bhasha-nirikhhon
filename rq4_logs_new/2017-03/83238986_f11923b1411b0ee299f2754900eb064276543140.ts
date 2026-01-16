import * as express from 'express';

import { Application,Request, Response } from 'express';
import {DataService} from "./dataservice";
import {Subscriber} from "rxjs";
import IRouterHandler = express.IRouterHandler;

class ApiRoutes {

    constructor(private app: Application) {
    }


    ApiOgScraper<T> (): any {

        return  this.app.route('/api/ogscraper').get(
            (req: Request, res: Response)=>{
                let data = new DataService();
                let url = req.param('url');
              data.getData(url).subscribe(
                    item =>  res.send(item)
                );
            }
        );
    }

}

export { ApiRoutes };