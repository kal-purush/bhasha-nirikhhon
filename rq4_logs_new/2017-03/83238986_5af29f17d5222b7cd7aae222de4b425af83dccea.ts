/**
 * Created by spotted on 26/02/17.
 */
import { Observable } from "rxjs/Observable";
// import { Observer } from 'rxjs/Observer';
// import { Subscription } from 'rxjs/Subscription';
import { Subscriber } from 'rxjs/Subscriber';
import "rxjs/add/observable/from";
import "rxjs/add/operator/publish";
import "rxjs/add/observable/defer";
import { Scraper } from './scraper';
import { PromiseObservable } from 'rxjs/observable/PromiseObservable';
import * as express from 'express';
import {ConnectableObservable, Observer} from "rxjs";

class DataService {


    constructor() {
    }

     getData<T>(url:string)  {

        const  scrapedData  = Scraper.scrape(url);


        //(2) same problem as (1) I cannot just use Observervable.from
         //prolly I will have to import {} and use FromObservable.create() as per from.d.ts

        let source = Observable.from(scrapedData);

        return source;


    }

}


export  { DataService };