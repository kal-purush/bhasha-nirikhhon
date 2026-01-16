// File: NavigationController.ts
/// <reference path="../Interfaces/IController" />
/// <reference path="../Views/NavigationView" />
/// <reference path="../../../../typings/jquery/jquery.d.ts"/>

import IController = require('../Interfaces/IController');

class NavigationController implements IController {

    constructor(){
        this.renderViews();
    }

    renderViews(){
        require(['../Views/NavigationView'], (View) => {
            var model = { navs:[
                { name: 'Home', path:'#', chosen: true },
                { name: 'About', path:'#/about' }
            ]};
            var view = new View('#nav-region', model);
            view.render();
        });
    }
}
export = NavigationController;