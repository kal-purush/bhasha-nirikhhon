export class fetch {

    constructor(private $http: angular.IHttpService, private $q: angular.IQService, private localStorageManager) { }

    fromService = (options: any) => {
        var deferred = this.$q.defer();
        this.$http({ method: options.method, url: options.url, data: options.data, params: options.params, headers: options.headers }).then((results) => {
            this.localStorageManager.put({ name: this.getCacheKey(options), value: results });
            deferred.resolve(results);
        }).catch((error) => {

        });
        return deferred.promise;
    }

    fromCacheOrService = (options: any) => {
        var deferred = this.$q.defer();
        var cachedData = this.localStorageManager.get({ name: this.getCacheKey(options) });
        if (!cachedData) {
            this.fromService(options).then((results) => {
                deferred.resolve(results);
            }).catch((error: Error) => {
                deferred.reject(error);
            });
        } else {
            deferred.resolve(cachedData);
        }
        return deferred.promise;
    }

    getCacheKey = options => options.params ? options.url + JSON.stringify(options.params) : options.url;

} 