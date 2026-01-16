module shop {
  'use strict';

  interface IPrice {
    price: number

  }

  interface IProduct {
    id: string
    name: string
    description: boolean

    inStock: boolean
    price: IPrice

  }

  export class DataProvider {
    private restangular:restangular.IService
    private $location: ng.ILocationService
    private UserLoginService: UserLoginService


    /** @ngInject */
    constructor(Restangular: restangular.IService, $location: ng.ILocationService, UserLoginService: UserLoginService) {
      this.restangular = Restangular;
      this.$location = $location;
      this.UserLoginService = UserLoginService;

      Restangular.setErrorInterceptor(
        function(response) {
          if (response.status == 401) {
            console.log("Login required... ");
            //$window.location.href='/login';
          } else if (response.status == 404) {
            console.log("Resource not available...");
          } else {
            console.log("Response received with HTTP error code: " + response.status );
          }
          return false; // stop the promise chain
        }
      );

    }

    devices (container:any): any {
      this.restangular.all('device').get('devices').then( (data: any) => {
        container.data = data;

      });
    }
  }
}