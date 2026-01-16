/**
 * Created by spotted on 26/02/17.
 */
import { Observable } from "rxjs/Observable";
import "rxjs/add/observable/from";

const ogs = require('open-graph-scraper');


class Scraper {


    public static scrape(url) {

        return  ogs({url: url}, // Settings object first
            function (er, res) {

                if (er == true) {
                    console.log(er);
                }
                else  {
                    return res;
                }


            }  // Callback)
        );

    }


}


class FbDataService {


    constructor() {
    }

     getData<T>(url:string)  {

        const  scrapedData  = Scraper.scrape(url);

        let source = Observable.from(scrapedData);

        return source;


    }

}


export  { FbDataService };