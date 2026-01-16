/* JS type wrappers */
///<reference path="../../typings/tsd.d.ts"/>

/* Project references */
/// <reference path="./Controllers/MapController.ts"/>

module XULAngular {
  export class Application {
    private instance: ng.IModule;

    constructor() {
      this.instance = angular.module('xul-app', []);

      this.instance.config(Config);

      this.instance.controller('MainController', MapController);
      this.instance.controller('MapController', MapController);
    }

    public get Instance(): ng.IModule {
      return this.instance;
    }
  }
}