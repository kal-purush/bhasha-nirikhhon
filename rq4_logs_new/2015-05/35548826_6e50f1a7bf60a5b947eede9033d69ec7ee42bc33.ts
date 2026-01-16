/// <reference path="../typings/angularjs/angular.d.ts"/>
/// <reference path="../typings/angularjs/angular-animate.d.ts"/>
/// <reference path="../typings/angular-ui-router/angular-ui-router.d.ts"/>
/// <reference path="../typings/jquery/jquery.d.ts"/>

declare var app: angular.IModule;
declare module 'app' {
  export = app;
}

interface lazyApp {
  factory(name:string, cb:()=>any);
  service(name:string, cb:()=>any);
  directive(name:string, cb:()=>any);
  filter(name:string, cb:()=>any);
  controller(name:string, cb:()=>any);
}
declare var lazy: lazyApp;
declare module 'lazy' {
  export = lazy;
}

declare var require;