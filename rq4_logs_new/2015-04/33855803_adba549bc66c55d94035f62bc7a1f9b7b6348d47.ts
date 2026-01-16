/// <reference path='../_all.d.ts' />

    'use strict';

    /**
     * Directive that executes an expression when the element it is applied to loses focus.
     */
     class Dashboard implements ng.IDirective{
       public restrict:string;
       public template:any;
       public controller:any;

       public static MODULE_ID:string = 'todomvc.directives.dashboard';
       public static SELECTOR:string = 'dashboard';

       constructor() {
         this.restrict = 'E';
         this.template = require('./dashboard.html');
         this.controller = require('../controllers/TodoCtrl').SELECTOR;
       }
     }

     angular.module(Dashboard.MODULE_ID, [
       require('../controllers/TodoCtrl').MODULE_ID,
       require('./TodoBlur').MODULE_ID,
       require('./TodoFocus').MODULE_ID
       ])
     .directive(Dashboard.SELECTOR, ()=>{
       return new Dashboard();
     });


export = Dashboard;