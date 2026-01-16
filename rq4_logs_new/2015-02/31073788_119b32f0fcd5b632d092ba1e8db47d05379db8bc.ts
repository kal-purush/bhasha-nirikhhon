// File: AboutController.ts
/// <reference path="../Interfaces/IController" />
/// <reference path="../Views/AboutView" />
/// <reference path="../../../../typings/jquery/jquery.d.ts"/>

import IController = require('../Interfaces/IController');

class AboutController implements IController {

    constructor(){}

    load(){
        require(['../Views/AboutView'], (AboutView) => {
            var view = new AboutView('#main-region');
            view.render();
        });
    }
}
export = AboutController;