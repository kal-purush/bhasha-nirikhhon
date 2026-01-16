
console.log('Now do the same in TS');

import config = require('config');
import { Observable } from 'rxjs/Rx';
import { Show } from './model/shows';
import { CollectionDetails } from './model/collectiondetails';
import { ShowList } from './lib/showlist';
import { ShowCollectionDetails } from './lib/showcollectiondetails';
import {Promise} from 'es6-promise';
var list = new ShowList(
    config.get('API.park.urlPUB7').toString(),
    config.get('APISignature.globalnavShows').toString()
    );

let showlist : Promise<Show[]>;
showlist = list.makeCall();

showlist.then(function(d){
    console.log('********************Warming started*****************');
    d.forEach(function(show){
        console.log(show.urlAlias);
        let showCollectionDetails = new ShowCollectionDetails(
            config.get('API.park.urlPUB7').toString(),
            config.get('APISignature.showDetail').toString().replace('<SHOW_NAME>',show.urlAlias)
        );

        let collectionList : Promise<CollectionDetails[]>;
        collectionList = showCollectionDetails.makeCall();

        collectionList.then(function(d){
            console.log(d);
        });
    })
});














