// App.js

/*jslint browser: true*/
/*global $, alert, define, require, google*/

define([
    'App', 'marionette', 'bootstrap', 'config/ajaxSetup', 'components/modalRegion'
], function (App, Marionette, bootstrap, ajaxSetup, modalRegion) {

    'use strict';

    //create new app
    var App = new Marionette.Application();

    //Store all custom route here
    App.History = [];

    //set main app regions
		App.addRegions({
        topMenuRegion: '#top-menu-region',
        topUserMenuregion: '#top-user-menu-region',
        topRegion: '#top-region',
        mainRegion: '#main-region',
        sideRegion: '#side-region',
        notificationRegion: '#notification-region'
    });

    //navigate
    App.navigate = function (route, options) {
				options || (options = {});
        Backbone.history.navigate(route, options);
        //App.storeRoute(route, options);
    };

    //get current route
    App.getCurrentRoute = function () {
        return Backbone.history.fragment
    };

    //if needed we can store the route history
    /*App.storeRoute = function (route, options) {
    App.History.push({
        name: route,
        args: options,
        fragment: App.getCurrentRoute()
        });
    }*/

    //handle app, new start, old close
    App.startSubApp = function (appName, args) {

        var currentApp = appName ? App.module(appName) : null;
        if (App.currentApp === currentApp) {
            return;
        }

        if (App.currentApp) {
            App.currentApp.stop();
        }

        App.currentApp = currentApp;
        if (currentApp) {
            currentApp.start(args);
        }
    };


    //app started load routers...
    App.on('start', function () {

        //remove static loader 
		$('.static-loader').removeClass('static-loader');
				
			 if (Backbone.history) {
					require([
							'appRouter'
					], function () {
							require([
								'apps/core/coreApp',
								'apps/articles/articlesApp',
								'apps/admin/adminApp',
								'apps/login/loginApp'
							], function () {
									Backbone.history.start();
							})
					});
        }
    });

    return window.App = App;
});