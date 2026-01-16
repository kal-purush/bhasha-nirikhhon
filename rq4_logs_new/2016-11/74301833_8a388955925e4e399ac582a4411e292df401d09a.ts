/**
 * Created by JJax on 28.11.2016.
 */

export class CustomFieldService {

    public async getCustomFields(fieldName: String, httpClient): Promise<String> {
        var fields = await this.getAllFields(httpClient);
        return new Promise<String>((resolve, reject) => {
            var toReturn = fields.filter((x) => {
                return x.name.toUpperCase().toString() === fieldName.toUpperCase();
            });

            if(toReturn){
                resolve((toReturn[0]).key);
            } else {
                reject("Cannot find epic link custom field");
            }
        });
    }

    private getAllFields(httpClient): Promise<any> {
        return new Promise((resolve, reject) => {
            httpClient.get({url: "rest/api/2/field"},
                (err, jiraRes, body) => {
                    if (err) {
                        reject(err);
                    }
                    resolve(JSON.parse(jiraRes.body));
                });
        });
    }
}