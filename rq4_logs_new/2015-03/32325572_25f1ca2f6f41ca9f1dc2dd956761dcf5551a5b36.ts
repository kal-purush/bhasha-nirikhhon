/// <reference path="../../vendor/restangular.d.ts" />

class verticalPageService {
    public static $inject = ['Restangular'];
    constructor(
        private Restangular: restangular.IService
    ){}

    getTweets (): restangular.IPromise<any> {
        return this.Restangular.one('tweets','response.json').get();
    }

}