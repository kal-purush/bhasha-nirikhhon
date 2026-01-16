module hateoas {
    'use strict';

    export class Client {
        constructor(private factory: ResourceFactory, private $http: ng.IHttpService) {
        }

        get(url: string, options?: Object): Resource {
            this.$http()
            return null;
        }

        post(url: string, data?: Object, options?: Object): Resource|void {
            return;
        }

        put(url: string, data?: Object, options?: Object): Resource|void {
            return;
        }

        patch(url: string, data?: Object, options?: Object): Resource|void {
            return;
        }

        del(url: string, options?: Object): void {
            return;
        }
    }
}