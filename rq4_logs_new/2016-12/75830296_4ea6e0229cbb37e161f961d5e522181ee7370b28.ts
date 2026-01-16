import config = require('config');
import { MixPanelReader } from '../mp/read';
import { HttpCaller } from '../lib/httpcaller';
import { onlyUnique } from '../util/filterunique'

export class Warmer{

    private MP: MixPanelReader;
    private urlList: Promise<string[]>;
    private httpCaller: HttpCaller;
    constructor(key,secret,caller) {
        this.MP = new MixPanelReader(
            config.get('key').toString(),
            config.get('secret').toString()
        );
        this.httpCaller = caller;
    }

    warm(eventQueryDate :string, showQuery :string): void{

        this.urlList = this.MP.getUrls(eventQueryDate, showQuery).catch(function(data){
            console.log('issue with Mixpanel read');
            return;
        });
        let caller = this.httpCaller;
        this.urlList.then(function(urlData){
            if(urlData != null){
                urlData.filter(onlyUnique).forEach(function(url){
                    caller.makecall('https://api.nbc.com'+url[0]).then(function(warmed){
                        console.log('Url to warm: ' + warmed.status + ' : ' + warmed.url);
                    })
                });
            }
            else{
                console.log('NO URL FOUND TO WARM BEACUSE OF MIXPANEL ISSUE');
            }
        });
    }
}