import {IConfig} from '../services/config';
import {Log} from '../models/log';

export class ElasticSearchLogsProvider {

    static $inject = ['$location', '$http'];

    constructor(private $location: ng.ILocationService, private $http: ng.IHttpService){
    }

    public get($scope, config: IConfig){
        var params = this.$location.search();
        var indices = this.getIndices(params.from, params.to);
        var query = {
            "size": 5000,
            "sort": [
                { "@timestamp": { "order": "asc" } },
                { "@order": { "order": "asc", "missing": "_first" } }
            ],
            "query": {
                "bool": {
                    "should": [
                        {
                            "term": {
                                "trace": params.traceid.substring(0, 8)
                            }
                        },
                        {
                            "term": {
                                "traceId": params.traceid
                            }
                        }
                    ]
                }
            }
        };
        $scope.traceid = params.traceid;
        $scope.logs = [];
        $scope.raw_content = "";
        $scope.querying = "Querying ...";
        angular.forEach(indices, (index_name) => {
            this.$http({
                url: config.logs_url + '/' + config.log_index_prefix["" + params.system] + index_name + '/' + 'LogEvent/_search',
                method: 'POST',
                data: query
            }).success((data: any) => {
                var raw = [];
                angular.forEach(data.hits.hits, (hit) => {
                    var log = new Log({ data: hit._source });
                    $scope.logs.push(log);
                    raw.push([log.data['@timestamp'],
                        log.data['Level'],
                        log.data['AppDomain'],
                        log.data['HostName'],
                        '[' + log.data['ThreadName'] + ']',
                        log.data['Message'],
                        log.data['Exception']
                    ].join("\t"));
                });
                $scope.raw_content = raw.join("\r\n");
                $scope.querying = "";
            }).error(function(data, status, headers, config) {
                $scope.querying = data;
            });
        });
    }

    getIndices(from: string, to: string): Array<string> {
        var indices = [];
        var fromDate = new Date(from);
        var toDate = new Date(to);
        indices.push(fromDate.toISOString().match(/(\d{4}\-\d{2}\-\d{2})T?/)[1].split('-').join('.'));
        indices.push(toDate.toISOString().match(/(\d{4}\-\d{2}\-\d{2})T?/)[1].split('-').join('.'));
        if (indices[0] == indices[1])
            indices.pop();
        return indices;
    }
}