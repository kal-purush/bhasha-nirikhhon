import { Request } from 'express';
import * as express from "express";

import { read } from 'fs';

export function ngApp(req: express.Request, res: express.Response, next:express.NextFunction) {
  
    renderPage(req, res);
}


function renderPage(req: express.Request, res: express.Response){


 function onHandleError(parentZoneDelegate, currentZone, targetZone, error)  {
    console.warn('Error in SSR, serving for direct CSR');
    res.sendFile('index.html', {root: './dist'});
    return false;
  }

 Zone.current.fork({ name: 'CSR fallback', onHandleError }).run(() => {
  res.render("index", {
    req,
    res,
    preboot:true,
    baseUrl: "/",
    requestUrl: req.originalUrl,
    originUrl: "http://localhost:3000"
  });
 });

}

// export function sitemapPage(req:express.Request, res:express.Response, next:express.NextFunction){
//   res.send(require('../../sitemap.xml'));
// }


