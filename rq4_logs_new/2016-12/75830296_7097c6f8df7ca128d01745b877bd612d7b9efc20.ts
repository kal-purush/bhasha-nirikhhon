const MixpanelExport = require('mixpanel-data-export-node');
export class MixPanelReader{
    /**
     *
     */
    panel: any;

    constructor(apiKey:string, apiSecret:string) {
        this.panel = new MixpanelExport({
            api_key: apiKey,
            api_secret: apiSecret
        });
    }

    getUrls(whereClause:string): Promise<string[]>{
        return this.panel.export({
            from_date: "2016-12-08",
            to_date: "2016-12-08",
            event: ["API Request"],
            where:  whereClause //'(properties["Resource"] == "shows" or properties["Resource"] == "videos" or properties["Resource"] == "collections") and (properties["Cached in Redis"] == "True") and (properties["Event Time: Minute of Hour"] == "15:00")'
            }).then(function(data) {
            //console.log(data);
                return data.map(x=> {
                    //console.log(x.properties.URL)
                    return  x.properties.URL;
                });
            });
    }
}