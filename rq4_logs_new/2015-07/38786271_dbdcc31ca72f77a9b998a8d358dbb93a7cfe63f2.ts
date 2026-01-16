///<reference path="..\..\..\typings\node\node.d.ts"/>
///<reference path="..\..\..\typings\express\express.d.ts"/>
///<reference path="auction_data_query.service.ts"/>

module dme.auctiondata {


   // var AuctionDataQueryService = require('AuctionDataQueryService');
    var express = require('express');
    export class AuctionDataRoute {

        constructor(app) {

        }

         public static listAeriePeakAuctions(req: any, res: any) {
             var service : AuctionDataQueryService = new AuctionDataQueryService(false);
             service.retrieveAHData("aerie-peak", (err, auctions) => {
                 console.log(auctions);
                 res.send(auctions);
             });
         }

    }


}