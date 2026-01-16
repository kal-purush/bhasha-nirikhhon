/// <reference path="../main" />

module chart {
    export class ChartService {
        private http:ng.IHttpService;

        constructor($http) {
            this.http = $http;
        }

        get(callback:(err:Error, data)=>void) {
            this.http.get('/hdlog')
                .success((res:any)=> {
                    callback(res.err, res.data);
                })
                .error((err)=> {
                    callback(new Error('http error'), err);
                })
        }

    }

    registerService('chartService', ChartService);
}